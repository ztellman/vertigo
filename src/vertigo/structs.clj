(ns vertigo.structs
  (:use
    potemkin)
  (:require
    [clojure.core.protocols :as proto]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p])
  (:import
    [java.nio
     ByteBuffer]))

;;;

(definterface+ IPrimitiveType
  (byte-size ^long [_]
    "The size of the primitive type, in bytes.")
  (fields [_]
    "The fields in the primitive struct.")
  (field-offset ^long [_ x]
    "Returns of the offset of the field within the struct, in bytes.")
  (field-type [_ x]
    "Returns the type of the field.")

  (write-value [_ byte-seq ^long offset x]
    "Writes the value to a byte-seq at the given byte offset.")
  (read-value [_ byte-seq ^long offset]
    "Reads the value from a byte-seq at the given byte-offset.")
  
  (read-form [_ byte-seq idx]
    "Returns an eval'able form for reading the struct off a byte-seq. If nil, falls back
     to `read-value`.")
  (write-form [_ byte-seq idx val]
    "Returns an eval'able form for writing the struct to a byte-seq.  If nil, falls back
     to `write-value`."))

(definterface+ IByteSeqWrapper
  (unwrap-byte-seq [_]))

;;;

(defn- byte-seq-wrapper-reduce
  ([byte-seq-wrapper f start]
     (proto/internal-reduce byte-seq-wrapper f start))
  ([byte-seq-wrapper f]
     (if (nil? byte-seq-wrapper)
       (f)
       (proto/internal-reduce (next byte-seq-wrapper) f (first byte-seq-wrapper)))))

;; a type that, given an IPrimitiveType, can treat a byte-seq as a sequence of that type
;; `stride` and `offset` are so that an inner type be iterated over without copying
(deftype ByteSeqWrapper
  [^long stride
   ^long offset
   ^vertigo.structs.IPrimitiveType type
   ^vertigo.bytes.IByteSeq byte-seq]

  IByteSeqWrapper
  (unwrap-byte-seq [_] byte-seq)

  clojure.lang.ISeq
  clojure.lang.Seqable
  clojure.lang.Sequential
  clojure.lang.Indexed
  
  (first [_]
    (read-value type byte-seq offset))
  (next [_]
    (when-let [byte-seq' (b/drop-bytes byte-seq stride)]
      (ByteSeqWrapper. stride offset type byte-seq')))
  (more [this]
    (or (next this) '()))
  (count [_]
    (p/div (b/byte-count byte-seq) stride))
  (nth [_ idx]
    (read-value type byte-seq (p/+ offset (p/* stride idx))))
  (nth [this idx default-value]
    (try
      (nth this idx)
      (catch IndexOutOfBoundsException e
        default-value)))
  (seq [this]
    this)

  proto/InternalReduce

  (internal-reduce [_ f start]
    (b/byte-seq-reduce byte-seq stride
      (fn [byte-seq idx]
        (read-value type byte-seq (p/+ offset (long idx))))
      f
      start))
  
  proto/CollReduce
  (coll-reduce [this f start]
    (byte-seq-wrapper-reduce this f start))
  
  (coll-reduce [this f]
    (byte-seq-wrapper-reduce this f)))

(defn wrap-byte-seq
  [type byte-seq]
  (ByteSeqWrapper. (byte-size type) 0 type byte-seq))

(defn marshal-seq
  "Converts a sequence into a marshalled version of itself."
  [type s]
  (let [cnt (count s)
        stride (byte-size type)
        byte-seq (-> (long cnt)
                   (p/* (long stride))
                   ByteBuffer/allocateDirect
                   b/byte-seq)]
    (loop [offset 0, s s]
      (when-not (empty? s)
        (write-value type byte-seq offset (first s))
        (recur (p/+ offset stride) (rest s))))
    (wrap-byte-seq type byte-seq)))

(defn lazily-marshal-seq
  ([type s]
     (lazily-marshal-seq type 4096 s))
  ([type ^long chunk-byte-size s]
     (let [stride (byte-size type)
           chunk-size (p/div chunk-byte-size stride)
           populate (fn populate [s]
                      (when-not (empty? s)
                        (let [nxt (delay (populate (drop chunk-size s)))
                              byte-seq (-> chunk-byte-size
                                         ByteBuffer/allocateDirect
                                         (b/lazy-byte-seq nxt))]
                          (loop [idx 0, offset 0, s s]
                            (if (or (p/== chunk-size idx) (empty? s))
                              (b/slice byte-seq 0 offset)
                              (do
                                (write-value type byte-seq offset (first s))
                                (recur (p/inc idx) (p/+ offset stride) (rest s))))))))]
       (wrap-byte-seq type (populate s)))))

;;;

(let [;; normal read-write
      s (fn [r w rev]
          `[(partial list '~r)
            (partial list '~w)
            (partial list '~rev)])

      ;; unsigned read-write
      u (fn [r w rev to-unsigned to-signed]
          `[(fn [b# idx#]
              (list '~to-unsigned (list '~r b# idx#)))
            (fn [b# idx# val#]
              (list '~w b# idx# (list '~to-unsigned val#)))
            (fn [x#]
              (list '~to-unsigned (list '~rev (list '~to-signed x#))))])
      types ['int8    1 (s `b/get-int8 `b/put-int8 `identity)
             'uint8   1 (u `b/get-int8 `b/put-int8 `identity `p/int8->uint8 `p/uint8->int8)
             'int16   2 (s `b/get-int16 `b/put-int16 `p/reverse-int16)
             'uint16  2 (u `b/get-int16 `b/put-int16 `p/reverse-int16 `p/int16->uint16 `p/uint16->int16)
             'int32   4 (s `b/get-int32 `b/put-int32 `p/reverse-int32)
             'uint32  4 (u `b/get-int32 `b/put-int32 `p/reverse-int32 `p/int32->uint32 `p/uint32->int32)
             'int64   8 (s `b/get-int64 `b/put-int64 `p/reverse-int64)
             'uint64  8 (u `b/get-int64 `b/put-int64 `p/reverse-int64 `p/int64->uint64 `p/uint64->int64)
             'float32 4 (s `b/get-float32 `b/put-float32 `p/reverse-float32)
             'float64 8 (s `b/get-float64 `b/put-float64 `p/reverse-float64)]]

  (doseq [[name size read-write-rev] (partition 3 types)]
    (let [[read-form write-form rev-form] (eval read-write-rev)]
      (eval
        (unify-gensyms
          `(let [[read-form# write-form# rev-form#] ~read-write-rev]

             ;; basic primitive
             (def ~name
               (reify
                 clojure.lang.Named
                 (getName [_#] ~(str name))
                 (getNamespace [_#] )

                 IPrimitiveType
                 (byte-size [_#]
                   ~size)
                 (fields [_#]
                   nil)
                 (field-offset [_# _#]
                   (throw (IllegalArgumentException.)))
                 (field-type [_# _#]
                   (throw (IllegalArgumentException.)))
                 (write-value [_# byte-seq## offset## x##]
                   ~(write-form `byte-seq## `offset## `x##))
                 (read-value [_# byte-seq## offset##]
                   ~(read-form `byte-seq## `offset##))
                 (read-form [_# b# idx#]
                   (read-form# b# idx#))
                 (write-form [_# b# idx# x#]
                   (write-form# b# idx# x#)))))))

      ;; big and little-endian primitives
      (when-not (#{'int8 'uint8} name)
        (doseq [[check name] (map list
                               [`b/big-endian? `b/little-endian?]
                               [(symbol (str name "-le")) (symbol (str name "-be"))])]
          (eval
            (unify-gensyms
              `(let [[read-form# write-form# rev-form#] ~read-write-rev]
                 (def ~name
                   (reify
                     clojure.lang.Named
                     (getName [_#] ~(str name))
                     (getNamespace [_#] )
                     
                     IPrimitiveType
                     (byte-size [_#]
                       ~size)
                     (fields [_#]
                       nil)
                     (field-offset [_# _#]
                       (throw (IllegalArgumentException.)))
                     (field-type [_# _#]
                       (throw (IllegalArgumentException.)))
                     (write-value [_# byte-seq## offset## x##]
                       (let [x## (if (~check byte-seq##)
                                   ~(rev-form `x##)
                                   x##)]
                         ~(write-form `byte-seq## `offset## `x##)))
                     (read-value [_# byte-seq## offset##]
                       (let [x## ~(read-form `byte-seq## `offset##)]
                         (if (~check byte-seq##)
                           ~(rev-form `x##)
                           x##)))
                     (read-form [_# b# idx#]
                       (list 'let [`x## (read-form# b# idx#)]
                         (list 'if (list ~check b#)
                           (rev-form# `x##)
                           `x##)))
                     (write-form [_# b# idx# x#]
                       (list 'let [`x## (list 'if (list ~check b#)
                                          (list rev-form# x#)
                                          x#)]
                         (write-form# b# idx# `x##)))))))))))))

;;;

(def ^:dynamic *types*)

(defn typed-struct
  "A data structure with explicit types, meant to sit atop a byte-seq.  Fields must be keys, and types must
   implement IPrimitiveType.  For better error messages, all structs must be named.

   (typed-struct 'vec2 :x float32 :y float32)

   The resulting value implements IPrimitiveType, and can be used within other typed structs."
  [name & field+types]

  (assert (even? (count field+types)))
  
  (let [fields (->> field+types (partition 2) (map first))
        types (->> field+types (partition 2) (map second))]

    (assert (every? keyword? fields))

    (doseq [[field type] (map list fields types)]
      (when-not (instance? IPrimitiveType type)
        (throw (IllegalArgumentException. (str field " is not a valid type.")))))
    
    (let [offsets (->> types (map byte-size) (cons 0) (reductions +) butlast)
          byte-size (->> types (map byte-size) (apply +))
          type-syms (map #(symbol (str "t" %)) (range (count types)))]

      (binding [*types* types]
        (eval
          (unify-gensyms
            `(let [~@(interleave type-syms
                       (map
                         (fn [x] `(nth *types* ~x))
                         (range (count types))))]
               (reify
                 clojure.lang.Named
                 (getName [_#] ~(str name))
                 (getNamespace [_#] )
                 
                 IPrimitiveType
                 (byte-size [_#]
                   ~byte-size)
                 (fields [_#]
                   ~(vec fields))
                 (field-offset [_# k#]
                   (long
                     (case k#
                       ~@(interleave fields offsets))))
                 (field-type [_# k#]
                   (case k#
                     ~@(interleave
                         fields
                         type-syms)))
                 (write-value [_# byte-seq## offset## x##]
                   ~@(map
                       (fn [k offset x]
                         `(write-value ~x byte-seq## (p/+ (long offset##) ~offset) (get x## ~k)))
                       fields
                       offsets
                       type-syms))
                 (read-value [_# byte-seq# offset##]
                   (let [byte-seq## (b/slice byte-seq# offset## ~byte-size)]

                     ;; map structure that sits atop a slice of the byte-seq
                     (reify-map-type
                       (~'keys [_#]
                         ~(vec fields))
                       (~'get [_# k# default-value#]
                         (case k#
                           ~@(interleave
                               fields
                               (map
                                 (fn [x offset]
                                   `(read-value ~x byte-seq## ~offset))
                                 type-syms
                                 offsets))
                           default-value#))
                       (~'assoc [this# k# v#]
                         (assoc (into {} this#) k# v#))
                       (~'dissoc [this# k#]
                         (dissoc (into {} this#) k#)))))
                 (write-form [_# byte-seq# offset# x#]
                   nil)
                 (read-form [_# byte-seq# offset#]
                   nil)))))))))

(defmacro def-typed-struct

  [name & field+types]
  `(def ~name (typed-struct name ~@field+types)))
