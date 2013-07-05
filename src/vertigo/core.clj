(ns vertigo.core
  (:refer-clojure
    :exclude [get-in])
  (:use
    potemkin
    [potemkin.walk :only (prewalk)])
  (:require
    [byte-streams :as bytes]
    [vertigo.io :as io]
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

(defn- with-element-type [sym seq env]
  (let [v (element-type env seq)]
    (with-meta sym {(keyword (symbol (namespace v) (name v))) true})))

(defn- free-variable? [x]
  (when (symbol? x)
    (let [s (name x)]
      (boolean
        (or (= "_" s)
          (re-find #"^\?.*" s))))))

(defn- validate-lookup
  "Makes sure that the given field actually exists within the given type."
  [type field]
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
  "Walks the field sequence, returning a descriptor of offsets, lengths, and other useful
   information."
  [type fields]
  (let [{:keys [types type-form-fn offsets lengths]}
        (reduce
          (fn [{:keys [types type-form-fn offsets lengths]} field]
            (let [type (last types)
                  _  (validate-lookup type field)
                  inner-type (s/field-type type field)
                  has-fields? (instance? IFixedCompoundType type)]
              {:types (conj types inner-type)
               :type-form-fn (if-let [v (resolve-type-var inner-type)]
                               (constantly v)
                               (fn [x] `(s/field-type ~(type-form-fn x) ~field)))
               :offsets (conj offsets
                          (if (symbol? field)
                            `(p/* ~(s/byte-size (s/field-type type 0)) (long ~field))
                            (s/field-offset type field)))
               :lengths (conj lengths
                          (when (and has-fields? (s/has-field? type 0))
                            (p/div (s/byte-size type) (s/byte-size (s/field-type type 0)))))}))
          {:types [type], :type-form-fn identity, :offsets [],  :lengths []}
          fields)
        
        inner-type (or (last types) type)

        ;; take a bunch of numbers and forms, and collapse all the numbers together
        collapse-exprs #(concat
                          (->> % (remove number?))
                          (->> % (filter number?) (apply +) list (remove zero?)))

        num-prefixed-lookups (->> fields
                               (take-while (complement free-variable?))
                               count)

        ;; the outermost boundaries of the lookup
        slice (when-not (zero? num-prefixed-lookups)
                {:offset (collapse-exprs (take num-prefixed-lookups offsets))
                 :length (s/byte-size (nth types num-prefixed-lookups))})

        contiguous-groups (fn contiguous-groups [pred s]
                            (let [s (drop-while (complement pred) s)]
                              (when-not (empty? s)
                                (cons (take-while pred s)
                                  (contiguous-groups pred (drop-while pred s))))))

        cross-sections (->> (map #(zipmap [:field :offset :length :stride] %&)
                                    fields
                                    offsets
                                    (map s/byte-size (rest types))
                                    (map s/byte-size types))
                         (drop num-prefixed-lookups)
                         (contiguous-groups #(not (free-variable? (:field %))))
                         (map (fn [field-descriptors]
                                {:offset-exprs (collapse-exprs (map :offset field-descriptors))
                                 :length (apply min (map :length field-descriptors))
                                 :stride (apply max (map :stride field-descriptors))})))]
    
    {:types (rest types)
     :cross-sections cross-sections
     :slice slice
     :type-form-fn type-form-fn
     :lengths lengths
     :inner-type inner-type
     :outer-type (nth types num-prefixed-lookups)
     :inlined? (instance? IFixedInlinedType inner-type)
     :offset-exprs (collapse-exprs offsets)}))

;;; 

(defn- field-operation
  [env x fields inlined-form non-inlined-form]
  (let [type-sym (element-type env x)
        type (resolve-type type-sym)
        x (with-meta x {})
        {:keys [inner-type inlined? type-form-fn offset-exprs]} (lookup-info type (rest fields))
        x-sym (if (symbol? x) x (gensym "x"))
        form ((if inlined?
                inlined-form
                non-inlined-form)
              (if inlined?
                inner-type
                (type-form-fn type-sym))
              x-sym
              `(s/unwrap-byte-seq ~x-sym)
              `(p/+ (s/index-offset ~x-sym ~(first fields)) ~@offset-exprs))]
    (unify-gensyms
      (if (= x x-sym)
        form
        `(let [~x-sym ~x] ~form)))))

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

(defn- over- [s lookup]
  (let [element-type (s/element-type s)
        type (s/array element-type (count s))
        {:keys [slice cross-sections inner-type]} (lookup-info type lookup)
        byte-seq (-> s bytes/to-byte-buffers b/to-byte-seq)
        byte-seq (if-let [{:keys [offset length]} slice]
                   (b/slice byte-seq (apply + offset) length)
                   byte-seq)]
    (->> cross-sections
      (reduce
        (fn [byte-seq {:keys [offset-exprs length stride]}]
          (b/cross-section byte-seq (apply + offset-exprs) length stride))
        byte-seq)
      (s/wrap-byte-seq inner-type))))

(defmacro over
  "Allows you to select a flattened subset of a sequence.  The `fields` are nested lookups,
   as you would use in `get-in`, but where a field describes an index in an array, the may be
   either '_' or a symbol prefixed with '?' to select all indices in that position.

   Take a 2x2 matrix of integers counting up from 0 to 3:

     ((0 1) (2 3))

   We get a flattened view of all integers within the matrix by marking both indices as free:

     (over s [_ _]) => (0 1 2 3)

   However, we can also iterate over only the elements in the first array:

     (over s [0 _]) => (0 1)

   Or only the first elements of all arrays:

     (over s [_ 0]) => (0 2)

   This syntax can be used in `doreduce` blocks to specify what subset to iterate over."
  [s fields]
  `(#'vertigo.core/over- ~s ~(vec (map #(if (free-variable? %) `'~% %) fields))))

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

;; todo: refactor this into to something not monstrous and terrifying
(defn- iteration-form [seq-bindings value-bindings env body]
  (let [seq-options (->> seq-bindings (drop-while (complement keyword?)) (apply hash-map))
        seq-bindings (take-while (complement keyword?) seq-bindings)
        elements (->> seq-bindings (partition 2) (map first))
        seqs (->> seq-bindings (take-while (complement keyword?)) (partition 2) (map second))
        accumulators (->> value-bindings (partition 2) (map first))
        initial-values (->> value-bindings (partition 2) (map second))
        over? (every? #(and (seq? %) (or (= 'over (first %)) (= #'vertigo.core/over (resolve (first %))))) seqs)
        seq-syms (repeatedly #(gensym "seq"))]
    (if-not over?

      ;; normal linear iteration
      (let [step (long (get seq-options :step 1))]
        (unify-gensyms
          `(let [~@(mapcat #(list (with-element-type %1 %2 env) %2) seq-syms seqs)
                 limit# (long ~(if-let [limit (seq-options :limit)]
                                 limit
                                 `(.count ~(with-meta (first seq-syms) {:tag "clojure.lang.Counted"}))))]
             (loop [idx## 0, ~@(interleave accumulators initial-values)]
               (if (< idx## limit#)
                 (let [~@(interleave elements (map (fn [s] `(get-in ~s [idx##])) seq-syms))]
                   ~(walk-return-exprs
                      (fn [x]
                        (cond
                          
                          (and (seq? x) (= 'break (first x)))
                          (if (= 2 (count x))
                            (second x)
                            `(vector ~@(rest x)))
                          
                          (= 1 (count accumulators))
                          `(recur (p/+ idx## ~step) ~x)
                          
                          (not= (count accumulators) (count x))
                          (throw
                            (IllegalArgumentException.
                              (str "expected " (count accumulators) " return values, got " (count x))))
                          
                          :else
                          `(recur (p/+ idx## ~step) ~@x)))
                      `(do ~@body)))
                 ~(if (= 1 (count accumulators))
                    (first accumulators)
                    `(vector ~@accumulators)))))))

      ;; multi-dimensional iteration
      (let [fields (map last seqs)
            seqs (map second seqs)
            iterators (->> fields
                        first
                        (filter free-variable?)
                        (map #(if (= '_ %) (gensym "?itr__") %)))
            fields (map
                     #(first
                        (reduce
                          (fn [[fields iterators] field]
                            (if (free-variable? field)
                              [(conj fields (first iterators)) (rest iterators)]
                              [(conj fields field) iterators]))
                          [[] iterators]
                          %))
                     fields)
            {:keys [lengths]} (lookup-info
                                (s/array
                                  (->> seqs first (element-type env) resolve-type)
                                  Integer/MAX_VALUE)
                                (first fields))
            iterator->step (if-let [step (seq-options :step)]
                             (if (= 1 (count iterators))
                               {(first iterators) step}
                               (throw (IllegalArgumentException.
                                        ":step is ambiguous when there are multiple free variables.")))
                             (merge
                               (zipmap iterators (repeat 1))
                               (seq-options :steps)))
            iterator->limit (if-let [limit (seq-options :limit)]
                              (if (= 1 (count iterators))
                                {(first iterators) limit}
                                (throw (IllegalArgumentException.
                                         ":limit is ambiguous when there are multiple free variables.")))
                              (-> (zipmap (rest (first fields)) (rest lengths))
                                (assoc (ffirst fields) `cnt##)
                                (merge (seq-options :limits))
                                (select-keys iterators)))
            iterator->limit-sym (zipmap
                                  (->> iterator->limit keys (remove #(number? (iterator->limit %))))
                                  (repeatedly #(gensym "lim__")))
            iterator->limit-val (merge iterator->limit iterator->limit-sym)

            body `(let [~@(interleave
                            elements
                            (map (fn [s fields] `(get-in ~s [~@fields])) seq-syms fields))]
                    ~(walk-return-exprs
                       (fn [x]
                         (cond
                             
                           (and (seq? x) (= 'break (first x)))
                           (if (= 2 (count x))
                             (second x)
                             `(vector ~@(rest x)))
                             
                           (= 1 (count accumulators))
                           `(recur ~@iterators ~x)
                             
                           (not= (count accumulators) (count x))
                           (throw
                             (IllegalArgumentException.
                               (str "expected " (count accumulators) " return values, got " (count x))))
                             
                           :else
                           `(recur ~@iterators ~@x)))
                       `(do ~@body)))

            root-iterator (last iterators)]

        (unify-gensyms
          `(let [~@(mapcat #(list (with-element-type %1 %2 env) %2) seq-syms seqs)
                 cnt## (long (.count ~(with-meta (first seqs) {:tag "clojure.lang.Counted"})))
                 ~@(apply concat
                     (map
                       (fn [[i sym]]
                         `(~sym ~(iterator->limit i)))
                       iterator->limit-sym))]
             (loop [~@(interleave (butlast iterators) (repeat 0))
                    ~root-iterator ~(- (iterator->step root-iterator))
                    ~@(interleave
                        accumulators
                        initial-values)]
               (let [~root-iterator (p/+ ~root-iterator ~(iterator->step root-iterator))
                     ~@(apply concat
                         (map
                           (fn [[i j]]
                             `(~j (if (>= ~i ~(iterator->limit-val i))
                                    (p/+ ~j ~(iterator->step j))
                                    ~j)))
                           (partition 2 1 (reverse iterators))))
                     ~@(apply concat
                         (map
                           (fn [i]
                             `(~i (if (>= ~i ~(iterator->limit-val i))
                                    0
                                    ~i)))
                           (rest iterators)))]
                 (if (< ~(first iterators) ~(iterator->limit-val (first iterators)))
                   ~body
                   ~(if (= 1 (count accumulators))
                      (first accumulators)
                      `(vector ~@accumulators)))))))))))

(defmacro doreduce
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
       (+ x sum)"
  [seq-bindings value-bindings & body]
  (iteration-form seq-bindings value-bindings &env body))
