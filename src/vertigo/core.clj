(ns vertigo.core
  (:refer-clojure
    :exclude [get-in])
  (:use
    potemkin
    [potemkin.walk :only (prewalk)])
  (:require
    [vertigo.structs :as s]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p])
  (:import
    [clojure.lang
     Compiler$LocalBinding]
    [vertigo.structs
     IFixedType
     IFixedInlinedType
     IFixedCompoundType]))

;;;

(defn- element-type
  "Check the local context and tag to determine element type."
  [env sym]
  (let [type-meta (fn [m]
                    (->> (keys m)
                      (filter keyword?)
                      (filter #(true? (m %)))
                      (map #(symbol (namespace %) (name %)))
                      (filter #(when-let [x (resolve %)]
                                 (instance? IFixedType @x)))
                      first))]
    (or
      (type-meta (meta sym))
      (when-let [^Compiler$LocalBinding binding (get env sym)]
        (type-meta (meta (.sym binding))))
      (type-meta (meta (resolve sym)))
      (throw (IllegalArgumentException. (str "cannot determine type of '" sym "'"))))))

(defn- resolve-type
  [type-sym]
  @(resolve type-sym))

(defn- resolve-type-var
  "Try to backtrace the value to the corresponding var."
  [type]
  (when-let [v (resolve (symbol (namespace type) (name type)))]
    (when (= @v type)
      v)))

(defn- validate-lookup [type field]
  ;; can't go any deeper
  (when-not (instance? IFixedCompoundType type)
    (throw (IllegalArgumentException. (str "Invalid field '" field "' for non-compound type " (name type)))))
  
  ;; if it's a symbol, it must be for an index
  (when (and (symbol? field) (not (s/has-field? type 0)))
    (throw (IllegalArgumentException. (str "'" field "' is assumed to be numeric, which isn't accepted by " (name type)))))

  ;; invalid field
  (when (and (or (keyword? field) (number? field)) (not (s/has-field? type field)))
    (throw (IllegalArgumentException. (str "Invalid field '" field "' for type " (name type))))))

(defn- lookup-info
  [type fields]
  (let [descriptor (reduce
                     (fn [{:keys [types type-form-fn offset-exprs lengths]} field]
                       (let [type (last types)
                             _  (validate-lookup type field)
                             inner-type (s/field-type type field)
                             has-fields? (instance? IFixedCompoundType type)]
                         {:types (conj types inner-type)
                          :type-form-fn (if-let [v (resolve-type-var inner-type)]
                                          (constantly v)
                                          (fn [x] `(s/field-type ~(type-form-fn x) ~field)))
                          :offset-exprs (conj offset-exprs
                                          (if (symbol? field)
                                            `(p/* ~(s/byte-size (s/field-type type 0)) (long ~field))
                                            (s/field-offset type field)))
                          :lengths (conj lengths
                                     (when (and has-fields? (s/has-field? type 0))
                                       (p/div (s/byte-size type) (s/byte-size (s/field-type type 0)))))}))
                     {:types [type], :type-form-fn identity, :offset-exprs [], :lengths []}
                     fields)
        inner-type (or (last (:types descriptor)) type)]
    (-> descriptor
      (update-in [:types] rest)
      (update-in [:offset-exprs]
        #(concat
           (->> % (remove number?))
           (->> % (filter number?) (apply +) list (remove zero?))))
      (assoc
        :inner-type inner-type
        :inlined? (instance? IFixedInlinedType inner-type)))))

;;;

(defn- field-operation
  [env x fields inlined-form non-inlined-form]
  (let [type-sym (element-type env x)
        type (resolve-type type-sym)
        x (with-meta x {})
        {:keys [inner-type inlined? type-form-fn offset-exprs]} (lookup-info type (rest fields))]
    (unify-gensyms
      `(let [x## ~x]
         ~((if inlined?
             inlined-form
             non-inlined-form)
           (if inlined?
             inner-type
             (type-form-fn type-sym))
           `x##
           `(s/unwrap-byte-seq x##)
           `(p/+ (s/index-offset x## ~(first fields)) ~@offset-exprs))))))

(defmacro get-in
  "Like `get-in`, but for sequences of typed-structs. The sequence `s` must be keyword type-hinted with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields]
  (field-operation &env s fields
    (fn [type seq byte-seq offset]
      (s/read-form type byte-seq offset))
    (fn [type seq byte-seq offset]
      `(s/read-value ~type ~byte-seq ~offset))))

(defmacro set-in!
  "Like `assoc-in`, but for sequences of typed-structs. The sequence `s` must be keyword type-hinted with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields val]
  (field-operation &env s fields
    (fn [type seq byte-seq offset]
      `(do
         ~(s/write-form type byte-seq offset val)
         nil))
    (fn [type seq byte-seq offset]
      `(do
         (s/write-value ~type ~byte-seq ~offset ~val)
         nil))))

(defmacro update-in!
  "Like `update-in`, but for sequences of typed-structs. The sequence `s` must be keyword type-hinted with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields f & args]
  (field-operation &env s fields
    (fn [type seq byte-seq offset]
      `(let [offset## ~offset
             byte-seq## ~byte-seq
             val## ~(s/read-form type `byte-seq## `offset##)
             val'## (~f val## ~@args)]
         ~(s/write-form type `byte-seq## `offset## `val'##)
         nil))
    (fn [type seq byte-seq offset]
      (let [offset# ~offset
            byte-seq# ~byte-seq
            element-type# ~type
            val# (s/read-value element-type# byte-seq# offset#)
            val'# (~f val# ~@args)]
        (s/write-value element-type# byte-seq# offset# val'#)
        nil))))

;;;

(defmulti ^:private walk-return-exprs
  (fn [f x]
    (if (seq? x)
      (first x)
      ::default))
  :default ::default)

(defmethod walk-return-exprs ::default [f x]
  (f x))

(defn- walk-last [f x]
  (concat
    (butlast x)
    [(walk-return-exprs f (last x))]))

(defmethod walk-return-exprs 'do [f x]
  (walk-last f x))

(defmethod walk-return-exprs 'let* [f x]
  (walk-last f x))

(defmethod walk-return-exprs 'loop* [f x]
  (walk-last f x))

(defmethod walk-return-exprs 'if [f [_ test then else]]
  `(if ~test
     ~(walk-return-exprs f then)
     ~(walk-return-exprs f else)))

(defmethod walk-return-exprs 'fn* [f x]
  (let [header (take-while (complement seq?) x)
        body   (drop (count header) x)]
    `(~@header ~@(map #(concat (butlast %) [(walk-return-exprs f x)]) body))))



#_(defmacro doreduce
  "A combination of `doseq` and `reduce`, this is a primitive for efficient batch operations over sequences.

   `reduce-over` takes two binding forms, one for sequences that mirrors `doseq`, and a second for accumulators
   that mirrors `loop`.  If there is only one accumulator, the body must return the new value for the accumulator.
   If there are multiple accumulators, it must return a vector containing values for each.

   So we can easily calculate the sum of a sequence:

     (reduce-over [x s] [sum 0]
       (+ x sum))

   We can also sum together two sequences:

     (reduce-over [x a, y b] [sum 0]
       (+ x y sum))

   And we can also calculate the product and sum at the same time:

     (reduce-over [x s] [sum 0, product 1]
       [(+ x sum) (* x product)])

   We can also iterate over particular fields or arrays within a sequence:

     (reduce-over [x (over s [_ :a :b])] [sum 0] 
       (+ x sum)

   This gives us the sum of (get-in [i :a :b]) for each index within the sequence.  The * symbol denotes the free variable
   that is being iterated over.  If there is an inner array, we can also iterate over that instead:


   This will give us the sum of (get-in [0 :z x]) for each x within the inner array."
  [seq-bindings value-bindings & body]
  (let [element-syms (->> seq-bindings (partition 2) (map first))
        seqs (->> seq-bindings (partition 2) (map second))
        value-syms (->> value-bindings (partition 2) (map first))
        types (map element-type seqs) 
        resolved-types (map resolve-type types)]
    (cond
      ;; multi-index iteration
      (every? vector? element-syms)
      (unify-gensyms
        )
      
     (unify-gensyms
       `(let [len# ~(let [lookup (first seq-lookups)]
                      (if (= '* (first lookup))
                        `(count ~(first seqs))
                        (inner-type-length
                          (first resolved-types)
                          (->> lookup rest (take-while #(not= '* %))))))]
          (loop [~'&idx 0 ~@value-bindings]
            (if (p/== ~'&idx len#)
              ~(if (= 1 (count value-syms))
                 (first value-syms)
                 `(vector ~@value-syms))
              (let [~@(interleave
                        element-syms
                        (map
                          (fn [type x lookup]
                            `(vertigo.core/get-in
                               ~(with-meta x {(keyword type) true})
                               ~(vec (map #(if (= '* %) '&idx %) lookup))))
                          types
                          seqs
                          seq-lookups))]
                ~(walk-return-exprs
                   (fn [x]
                     (cond
                      
                       (= 1 (count value-syms))
                       `(recur (p/inc ~'&idx) ~x)
                      
                       (not (= (count x) (count value-syms)))
                       (throw
                         (IllegalArgumentException.
                           (str "expected " (count value-syms) " return values, got " (count x))))
                      
                       :else
                       `(recur (p/inc ~'&idx) ~@x)))
                   `(do ~@body))))))))))
