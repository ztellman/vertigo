(ns vertigo.core
  (:refer-clojure
    :exclude [get-in reduce])
  (:use
    potemkin)
  (:require
    [vertigo.structs :as s]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p])
  (:import
    [vertigo.structs
     IFixedType
     IFixedInlinedType
     IFixedCompoundType]))

;;;

(defn marshal-seq
  "Converts a sequence into a marshaled version of itself."
  ([type s]
     (marshal-seq type true s))
  ([type direct? s]
     (let [cnt (count s)
           stride (s/byte-size type)
           allocate (if direct? b/direct-buffer b/buffer)
           byte-seq (-> (long cnt) (p/* (long stride)) allocate b/byte-seq)]
       (loop [offset 0, s s]
         (when-not (empty? s)
           (s/write-value type byte-seq offset (first s))
           (recur (p/+ offset stride) (rest s))))
       (s/wrap-byte-seq type byte-seq))))

(defn lazily-marshal-seq
  "Lazily converts a sequence into a marshaled version of itself."
  ([type s]
     (lazily-marshal-seq type 4096 false s))
  ([type ^long chunk-byte-size direct? s]
     (let [stride (s/byte-size type)
           chunk-size (p/div chunk-byte-size stride)
           allocate (if direct? b/direct-buffer b/buffer)
           populate (fn populate [s]
                      (when-not (empty? s)
                        (let [nxt (delay (populate (drop chunk-size s)))
                              byte-seq (-> chunk-byte-size allocate (b/lazy-byte-seq nxt))]
                          (loop [idx 0, offset 0, s s]
                            (if (or (p/== chunk-size idx) (empty? s))
                              (b/slice byte-seq 0 offset)
                              (do
                                (s/write-value type byte-seq offset (first s))
                                (recur (p/inc idx) (p/+ offset stride) (rest s))))))))]
       (s/wrap-byte-seq type (populate s)))))

;;;

(defn- inner-type
  "Returns a tuple of [inner-type offset-exprs]."
  [type fields]
  (let [[type offsets]
        (clojure.core/reduce
          (fn [[type offsets] field]
            (cond

              ;; can't go any deeper
              (not (instance? IFixedCompoundType type))
              (throw (IllegalArgumentException. (str "Invalid field '" field "' for non-compound type " (name type))))
              
              ;; symbol, assumed to be an index
              (not (or (number? field) (keyword? field)))
              (if-not (s/has-field? type 0)
                (throw (IllegalArgumentException. (str "'" field "' is assumed to be numeric, which isn't accepted by " (name type))))
                [(s/field-type type field) (conj offsets `(p/* ~(s/field-offset type 1) ~field))])
              
              ;; keyword or number
              (s/has-field? type field)
              [(s/field-type type field) (conj offsets (s/field-offset type field))]
              
              :else
              (throw (IllegalArgumentException. (str "Invalid field '" field "' for type " (name type))))))
          [type []]
          fields)]
    [type (cons
            (->> offsets (filter number?) (apply +))
            (->> offsets (remove number?)))]))

(defn- field-operation [op-name x fields inlined-form non-inlined-form]
  (let [type (if-let [type (:tag (meta x))]
               @(resolve type)
               (throw (IllegalArgumentException. (str "First argument to " op-name  " must be hinted with element type"))))]

    (let [[inner-type offsets] (inner-type type (rest fields))
          x (with-meta x {})]
      (unify-gensyms
        `(let [x## ~x]
           ~((if (instance? IFixedInlinedType inner-type)
               inlined-form
               non-inlined-form)
             inner-type `x## `(s/unwrap-byte-seq x##) `(p/+ (s/index-offset x## ~(first fields)) ~@offsets)))))))

(defmacro get-in
  "Like `get-in`, but for sequences of typed-structs. The sequence `s` must be type-hinted with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields]
  (field-operation "get-in*" s fields
    (fn [type seq byte-seq offset]
      (s/read-form type byte-seq offset))
    (fn [type seq byte-seq offset]
      `(s/read-value (s/element-type ~seq) ~byte-seq ~offset))))

(defmacro set-in!
  "Like `assoc-in`, but for sequences of typed-structs. The sequence `s` must be type-hinted with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields val]
  (field-operation "set-in!" s fields
    (fn [type seq byte-seq offset]
      `(do
         ~(s/write-form type byte-seq offset val)
         nil))
    (fn [type seq byte-seq offset]
      `(do
         (s/write-value (s/element-type ~seq) ~byte-seq ~offset ~val)
         nil))))

(defmacro update-in!
  "Like `update-in`, but for sequences of typed-structs. The sequence `s` must be type-hints with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields f & args]
  (field-operation "update-in!" s fields
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
            element-type# (s/element-type ~seq)
            val# (s/read-value element-type# byte-seq# offset#)
            val'# (~f val# ~@args)]
        (s/write-value element-type# byte-seq# offset# val'#)
        nil))))
