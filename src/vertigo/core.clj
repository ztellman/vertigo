(ns vertigo.core
  (:refer-clojure
    :exclude [get-in every? flush])
  (:use
    potemkin
    [potemkin.walk :only (prewalk)])
  (:require
    [byte-streams :as bytes]
    [vertigo.structs :as s]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p])
  (:import
    [java.io
     File]
    [java.nio
     MappedByteBuffer]
    [clojure.lang
     Compiler$LocalBinding]
    [vertigo.structs
     IFixedType
     IFixedInlinedType
     IFixedCompoundType]))

;;;

(defn flush
  "If necessary, flushes changes that have been made to the byte sequence."
  [x]
  (let [byte-seq (s/unwrap-byte-seq x)]
    (when-let [flush-fn (b/flush-fn byte-seq)]
      (flush-fn byte-seq))))

(defn- safe-chunk-size [type ^long chunk-size]
  (p/* (s/byte-size type) (p/div chunk-size (s/byte-size type))))

(defn wrap
  "Wraps `x`, treating it as a sequence of `type`.  If `x` is a string, it will be treated
   as a filename.  If `x` can be converted to a ByteBuffer without any copying (byte-arrays,
   files, etc.), this is an O(1) operation.

   This will behave like a normal, immutable sequence of data, unless `set-in!` or `update-in!`
   are used.  It can be used with `doreduce` for efficient iteration or reduction.

   This returns an object that plays well with `byte-streams/to-*`, so the inverse operation is
   simply `byte-streams/to-byte-buffer`."
  ([type x]
     (wrap type bytes nil))
  ([type x
    {:keys [direct? chunk-size writable?]
     :or {direct? false
          chunk-size (if (string? x)
                       (int 2e9)
                       (int 1e6))
          writable? true}
     :as options}]
     (let [x' (if (string? x) (File. ^String x) x)
           chunk-size (safe-chunk-size type chunk-size)]
       (let [bufs (bytes/to-byte-buffers x' options)]
         (s/wrap-byte-seq type
           (if (empty? (rest bufs))
             (b/byte-seq (first bufs))
             (b/to-chunked-byte-seq bufs))
           (when (satisfies? bytes/Closeable bufs)
             (fn [] (bytes/close bufs)))
           (when (or (instance? File x) (string? x))
             (fn [] (map #(.force ^MappedByteBuffer %) bufs))))))))

(defn marshal-seq
  "Converts a sequence into a marshaled version of itself."
  ([type s]
     (marshal-seq type s nil))
  ([type s {:keys [direct?] :or {direct? true}}]
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
     (lazily-marshal-seq type s nil))
  ([type s {:keys [chunk-size direct?] :or {chunk-size 65536}}]
     (let [chunk-size (long (safe-chunk-size type chunk-size))
           allocate (if direct? b/direct-buffer b/buffer)
           stride (s/byte-size type)
           populate (fn populate [s]
                      (when-not (empty? s)
                        (let [remaining (promise)
                              nxt (delay (populate @remaining))
                              byte-seq (-> chunk-size allocate (b/chunked-byte-seq nxt))]
                          (loop [idx 0, offset 0, s s]
                            (cond

                              (empty? s)
                              (do
                                (deliver remaining nil)
                                (b/slice byte-seq 0 offset))

                              (p/== chunk-size offset)
                              (do
                                (deliver remaining s)
                                byte-seq)

                              :else
                              (do
                                (s/write-value type byte-seq offset (first s))
                                (recur (p/inc idx) (p/+ offset stride) (rest s))))))))]
       (s/wrap-byte-seq type (populate s)))))

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
  (let [{:keys [types type-form-fn offsets lengths limits]}
        (reduce
          (fn [{:keys [types type-form-fn offsets lengths limits]} field]
            (let [type (last types)
                  _  (validate-lookup type field)
                  inner-type (s/field-type type field)
                  has-fields? (instance? IFixedCompoundType type)
                  length (when (and has-fields? (s/has-field? type 0))
                           (p/div (s/byte-size type) (s/byte-size (s/field-type type 0))))]
              {:types (conj types inner-type)
               :type-form-fn (if-let [v (resolve-type-var inner-type)]
                               (constantly v)
                               (fn [x] `(s/field-type ~(type-form-fn x) ~field)))
               :offsets (conj offsets
                          (if (symbol? field)
                            `(p/* ~(s/byte-size (s/field-type type 0)) (long ~field))
                            (s/field-offset type field)))
               :lengths (conj lengths length)
               :limits (if (symbol? field)
                         (merge-with min limits {field length})
                         limits)}))
          {:types [type], :type-form-fn identity, :offsets [],  :lengths [], :limits {}}
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
     :offset-exprs (collapse-exprs offsets)
     :validation-exprs (map
                         (fn [[field length]]
                           `(when (>= (long ~field) ~length)
                              (throw (IndexOutOfBoundsException. (str ~(str field) ": " ~field)))))
                         limits)}))

;;; 

(defn- field-operation
  [env x fields validate? inlined-form non-inlined-form]
  (let [type-sym (element-type env x)
        type (resolve-type type-sym)
        x (with-meta x {})
        {:keys [inner-type inlined? type-form-fn offset-exprs validation-exprs]} (lookup-info type (rest fields))
        x-sym (if (symbol? x) x (gensym "x"))
        form ((if inlined?
                inlined-form
                non-inlined-form)
              (if inlined?
                inner-type
                (type-form-fn type-sym))
              x-sym
              `(s/unwrap-byte-seq ~x-sym)
              `(p/+ (s/index-offset ~x-sym ~(first fields)) ~@offset-exprs))
        validation (when validate?
                     (concat
                       validation-exprs
                       (when (symbol? (first fields))
                         [`(when (>= (long ~(first fields))
                                   (.count ~(with-meta x-sym {:tag "clojure.lang.Counted"})))
                             (throw
                               (IndexOutOfBoundsException.
                                 (str ~(str (first fields)) ": " ~(first fields)))))])))]
    (unify-gensyms
      (if (= x x-sym)
        `(do
           ~@validation
           ~form)
        `(let [~x-sym ~x]
           ~@validation
           ~form)))))

(defn- get-in-form [s fields env validate?]
  (field-operation env s fields validate?
    (fn [type seq byte-seq offset]
      (s/read-form type byte-seq offset))
    (fn [type seq byte-seq offset]
      `(s/read-value ~type ~byte-seq ~offset))))

(defmacro get-in
  "Like `get-in`, but for sequences of typed-structs. The sequence `s` must be keyword type-hinted with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields]
  (get-in-form s fields &env (not b/use-unsafe?)))

(defmacro get-in'
  "An unsafe version of `vertigo.core/get-in` which doesn't do runtime index checking."
  [s fields]
  (get-in-form s fields &env false))

(defn- set-in-form [s fields val env validate?]
  (field-operation env s fields validate?
    (fn [type seq byte-seq offset]
      `(do
         ~(s/write-form type byte-seq offset val)
         nil))
    (fn [type seq byte-seq offset]
      `(do
         (s/write-value ~type ~byte-seq ~offset ~val)
         nil))))

(defmacro set-in!
  "Like `assoc-in`, but for sequences of typed-structs. The sequence `s` must be keyword type-hinted with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields val]
  (set-in-form s fields val &env (not b/use-unsafe?)))

(defmacro set-in!'
  "An unsafe version of `set-in!` which doesn't do runtime index checking."
  [s fields val]
  (set-in-form s fields val &env (not b/use-unsafe?)))

(defn- update-in-form [s fields f args env validate?]
  (field-operation env s fields false
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

(defmacro update-in!
  "Like `update-in`, but for sequences of typed-structs. The sequence `s` must be keyword type-hinted with the element type,
   which allows for compile-time calculation of the offset, and validation of the lookup."
  [s fields f & args]
  (update-in-form s fields f args &env (not b/use-unsafe?)))

(defmacro update-in!'
  "An unsafe version of `update-in!` which doesn't do runtime index checking."
  [s fields f & args]
  (update-in-form s fields f args &env false))

;;;

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

     `((0 1) (2 3))`

   We get a flattened view of all integers within the matrix by marking both indices as free:

     `(over s [_ _])` => `(0 1 2 3)`

   However, we can also iterate over only the elements in the first array:

     `(over s [0 _])` => `(0 1)`

   Or only the first elements of all arrays:

     `(over s [_ 0])` => `(0 2)`

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

(defn- iteration-arguments [seq-bindings value-bindings env]
  (let [seq-options (->> seq-bindings (drop-while (complement keyword?)) (apply hash-map))
        seq-bindings (take-while (complement keyword?) seq-bindings)
        elements (->> seq-bindings (partition 2) (map first))
        seqs (->> seq-bindings (take-while (complement keyword?)) (partition 2) (map second))
        value-syms (->> value-bindings (partition 2) (map first))
        initial-values (->> value-bindings (partition 2) (map second))
        seq-syms (repeatedly #(gensym "seq"))
        over? (clojure.core/every?
                #(and (seq? %)
                   (or (= 'over (first %))
                     (= #'vertigo.core/over (resolve (first %)))))
                seqs)
        arguments {:over? over?
                   :seqs (map
                           #(zipmap [:element :sym :expr] %&)
                           elements
                           seq-syms
                           seqs)
                   :values (map
                             #(zipmap [:initial :sym] %&)
                             initial-values
                             value-syms)}]
    (if-not over?

      (merge arguments
        {:step (long (get seq-options :step 1))
         :limit (get seq-options :limit)
         :count-expr `(long (.count ~(with-meta (first seqs) {:tag "clojure.lang.Counted"})))})

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
            lookup-info (map
                          (fn [seq fields]
                            (lookup-info
                              (s/array
                                (->> seq (element-type env) resolve-type)
                                Integer/MAX_VALUE)
                              fields))
                          seqs
                          fields) 
            lengths (map
                      (fn [seq-syms lengths]
                        (cons
                          `(.count ~(with-meta seq-syms {:tag "clojure.lang.Counted"}))
                          (rest lengths)))
                      seq-syms
                      (map :lengths lookup-info))
            steps (if-let [step (seq-options :step)]
                    (if (= 1 (count iterators))
                      [step]
                      (throw (IllegalArgumentException.
                               ":step is ambiguous when there are multiple free variables.")))
                    (map #(get (:steps seq-options) % 1) iterators))

            bounds (->> (mapcat (partial map vector) fields lengths)
                     (filter #(symbol? (first %)))
                     (map #(apply hash-map %))
                     (apply merge-with min))
            limits (merge bounds
                     (if-let [limit (seq-options :limit)]
                       (if (= 1 (count iterators))
                         {(first iterators) limit}
                         (throw (IllegalArgumentException.
                                  ":limit is ambiguous when there are multiple free variables.")))
                       (seq-options :limits)))]

        (doseq [k (keys limits)]
          (when (and
                  (number? (limits k))
                  (number? (bounds k))
                  (> (limits k) (bounds k)))
            (throw (IllegalArgumentException.
                     (str ":limit of " (limits k) " is greater than max value " (bounds k))))))
        
        (merge
          (update-in arguments [:seqs]
            (fn [seqs]
              (map
                #(-> %1
                   (assoc :fields %2)
                   (update-in [:expr] second))
                seqs
                fields)))
          (let [iterator-limits (map #(get limits %) iterators)]
            {:limits limits
             :iterators (map #(zipmap [:sym :length :step :limit :limit-sym] %&)
                          iterators
                          (first lengths)
                          steps
                          iterator-limits
                          (map #(when-not (number? %) (gensym "lmt__")) iterator-limits))}))))))

(defn- iteration-form [seq-bindings value-bindings env body]
  (let [{:keys [over?] :as arguments} (iteration-arguments seq-bindings value-bindings env)]
    (if-not over?

      ;; normal linear iteration
      (let [{:keys [step limit count-expr values seqs]} arguments]
        (unify-gensyms
          `(let [~@(mapcat
                     (fn [{:keys [sym expr]}]
                       (list (with-element-type sym expr env) expr))
                     seqs)
                 limit## ~(or limit count-expr)]

             ~@(when (and (not b/use-unsafe?) limit)
                 `((let [cnt# ~count-expr]
                     (when (>= limit## cnt#)
                       (throw (IndexOutOfBoundsException. (str cnt#)))))))
             
             (loop [idx## 0, ~@(mapcat
                                 (fn [{:keys [initial sym]}]
                                   [sym initial])
                                 values)]
               (if (< idx## limit##)
                 (let [~@(mapcat
                           (fn [{:keys [sym element]}]
                             [element `(get-in' ~sym [idx##])])
                           seqs)]
                   ~(walk-return-exprs
                      (fn [x]
                        (cond
                          
                          (and (seq? x) (= 'break (first x)))
                          (if (= 2 (count x))
                            (second x)
                            `(vector ~@(rest x)))
                          
                          (= 1 (count values))
                          `(recur (p/+ idx## ~step) ~x)
                          
                          (not= (count values) (count x))
                          (throw
                            (IllegalArgumentException.
                              (str "expected " (count values) " return values, got " (count x))))
                          
                          :else
                          `(recur (p/+ idx## ~step) ~@x)))
                      `(do ~@body)))
                 ~(if (= 1 (count values))
                    (:sym (first values))
                    `(vector ~@(map :sym values))))))))

      ;; multi-dimensional iteration
      (let [{:keys [seqs limits values iterators]} arguments
            body `(let [~@(mapcat
                           (fn [{:keys [sym element fields]}]
                             [element `(get-in' ~sym [~@fields])])
                           seqs)]
                    ~(walk-return-exprs
                       (fn [x]
                         (cond
                             
                           (and (seq? x) (= 'break (first x)))
                           (if (= 2 (count x))
                             (second x)
                             `(vector ~@(rest x)))
                             
                           (= 1 (count values))
                           `(recur ~@(map :sym iterators) ~x)
                             
                           (not= (count values) (count x))
                           (throw
                             (IllegalArgumentException.
                               (str "expected " (count values) " return values, got " (count x))))
                             
                           :else
                           `(recur ~@(map :sym iterators) ~@x)))
                       `(do ~@body)))
            root-iterator (last iterators)]

        (unify-gensyms
          `(do
             (let [~@(mapcat
                       (fn [{:keys [sym expr]}]
                         (list (with-element-type sym expr env) expr))
                       seqs)
                   cnt## ~(:limit (first iterators))
                   ~@(mapcat
                      (fn [{:keys [limit-sym limit]}]
                        (when limit-sym
                          [limit-sym limit]))
                      (butlast iterators))]

               ;; make sure limits are valid
               ~@(when-not b/use-unsafe?
                   (->> limits
                     (filter (fn [[sym lim]] (and (symbol? sym) (number? lim))))
                     (remove #(free-variable? (first %)))
                     (concat (->> iterators
                               (filter #(and (:limit-sym %) (not= (:limit %) (:length %))))
                               (map #(list (:limit-sym %) (:length %)))))
                     (map (fn [[sym limit]]
                            `(when (>= ~sym (long ~limit))
                               (throw (IndexOutOfBoundsException.
                                        (str ~(str sym) ": "(str ~sym)))))))))
               
               (loop [~@(interleave (map :sym (butlast iterators)) (repeat 0))
                      ~(:sym root-iterator) ~(- (:step root-iterator))
                      ~@(mapcat
                          (fn [{:keys [sym initial]}]
                            [sym initial])
                          values)]
                 (let [~(:sym root-iterator) (p/+ ~(:sym root-iterator) ~(:step root-iterator))
                      ~@(mapcat
                          (fn [[i j]]
                            `(~(:sym j)
                              (if (>= ~(:sym i) ~(or (:limit-sym i) (:limit i)))
                                (p/+ ~(:sym j) ~(:step j))
                                ~(:sym j))))
                          (partition 2 1 (reverse iterators)))
                      ~@(mapcat
                          (fn [i]
                            `(~(:sym i)
                              (if (>= ~(:sym i) ~(or (:limit-sym i) (:limit i)))
                                0
                                ~(:sym i))))
                          (rest iterators))]
                  (if (< ~(:sym (first iterators)) cnt##)
                    ~body
                    ~(if (= 1 (count values))
                       (:sym (first values))
                       `(vector ~@(map :sym values)))))))))))))

(defmacro doreduce
  "A combination of `doseq` and `reduce`, this is a primitive for efficient batch operations over sequences.

   `doreduce` takes two binding forms, one for sequences that mirrors `doseq`, and a second
   for accumulators that mirrors `loop`.  If there is only one accumulator, the body must
   return the new value for the accumulator.  If there are multiple accumulators, it must return
   a vector containing values for each.  This will not require actually allocating a vector,
   except for the final result.

   So we can easily calculate the sum of a sequence:

     `(doreduce [x s] [sum 0]
        (+ x sum))`

   We can also sum together two sequences:

     `(doreduce [x a, y b] [sum 0]
        (+ x y sum))`

   And we can also calculate the product and sum at the same time:

     `(doreduce [x s] [sum 0, product 1]
        [(+ x sum) (* x product)])`

   We can also iterate over particular fields or arrays within a sequence, using the `over`
   syntax.  This is faster than passing in a sequence which has been called with `over`
   elsewhere, and should be used inline where possible:

      `(doreduce [x (over s [_ :a :b])] [sum 0] 
         (+ x sum)`

   Both the `:step` and `:limit` for iteration may be specified:

       `(doreduce [x s, :step 3, :limit 10] [sum 0]
          (+ x sum))`

    This will only sum the `[0, 3, 6, 9]` indices.  If there are multiple iterators, the
    values must be specified using `:steps` and `:limits`:

       `(doreduce [x (over s [?i 0 ?j]), :steps {?i 2}, :limits {?j 20}] [sum 0]
          (+ x sum))`

    Limits and indices that are out of bounds will throw an exception at either compile or
    runtime, depending on when they can be resolved to a number."
  [seq-bindings value-bindings & body]
  (iteration-form seq-bindings value-bindings &env body))

;;;

(defmacro sum
  "Returns the sum of all numbers within the sequence.

     (sum s)

   or

     (sum s :step 2, :limit 10)"
  [s & options]
  `(doreduce [x# ~s ~@options] [sum# 0]
     (p/+ x# sum#)))

(defmacro every?
  "Returns true if `pred-expr` returns true for all `x` in `s`.

     (every? [x s] (pos? x))

   or

     (every? [x s, :limit 10] (even? x))"
  [[x s & options] pred-expr]
  `(doreduce [~x ~s ~@options] [bool# true]
     (if ~pred-expr
       true
       (break false))))

(defmacro any?
  "Returns true if `pred-expr` returns true for some `x` in `s`.

     (any? [x s] (zero? x))

   or

     (any? [x s, :step 42] (neg? x))"
  [[x s & options] pred-expr]
  `(doreduce [~x ~s ~@options] [bool# false]
     (if ~pred-expr
       (break true)
       false)))
