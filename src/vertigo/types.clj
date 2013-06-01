(ns vertigo.types
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
  (byte-size ^long [_] "The size of the primitive type, in bytes.")
  (fields [_] "The fields in the primitive struct.")
  (field-offset [_ x] "Returns of the offset of the field within the struct, in bytes.")
  (field-type [_ x] "Returns the type of the field.")
  (write-to-byte-seq [_ byte-seq ^long offset x] "Encodes the type to a byte-seq at the given byte offset.")
  (decode-byte-seq [_ byte-seq])
  (read-form [_ byte-seq idx])
  (write-form [_ byte-seq idx val]))

(defn encode-seq [type s]
  (let [cnt (count s)
        stride (byte-size type)
        byte-seq (-> (long cnt)
                   (p/* (long stride))
                   ByteBuffer/allocateDirect
                   b/byte-seq)]
    (loop [offset 0, s s]
      (when-not (empty? s)
        (write-to-byte-seq type byte-seq offset (first s))
        (recur (p/+ offset stride) (rest s))))
    byte-seq))

(defn lazily-encode-seq
  ([type s]
     (lazily-encode-seq type 4096 s))
  ([type ^long chunk-byte-size s]
     (when-not (empty? s)
       (let [stride (byte-size type)
             chunk-size (p/div chunk-byte-size stride)
             nxt (delay (lazily-encode-seq type chunk-byte-size (drop chunk-size s)))
             byte-seq (-> chunk-byte-size
                        ByteBuffer/allocateDirect
                        (b/lazy-byte-seq nxt))]
         (loop [idx 0, offset 0, s s]
           (if (p/|| (p/== chunk-size idx) (empty? s))
             (b/truncate byte-seq offset)
             (do
               (write-to-byte-seq type byte-seq offset (first s))
               (recur (p/inc idx) (p/+ offset stride) (rest s)))))))))

;;;

(defn- byte-seq-wrapper-reduce
  ([byte-seq-wrapper f start]
     (proto/internal-reduce byte-seq-wrapper f start))
  ([byte-seq-wrapper f]
     (if (nil? byte-seq-wrapper)
       (f)
       (proto/internal-reduce (next byte-seq-wrapper) f (first byte-seq-wrapper)))))

(defn byte-seq-decoder-form
  "Returns the form of a seq type that, given a byte-seq, acts as a sequence of the given primitive type."
  [name byte-size read-form]
  (unify-gensyms
    `(deftype ~name [^vertigo.bytes.IByteSeq byte-seq##]
       clojure.lang.ISeq
       clojure.lang.Seqable
       clojure.lang.Sequential
       
       (first [_]
         ~(read-form `(b/local-bytes byte-seq##) 0))
       (next [_]
         (when-let [byte-seq# (b/drop-bytes byte-seq## ~byte-size)]
           (new ~name byte-seq#)))
       (more [this#]
         (or (next this#) '()))
       (cons [this# x#]
         (cons x# this#))
       (seq [this#]
         this#)

       clojure.lang.Indexed
       (count [_]
         (p/div (b/byte-count byte-seq##) ~byte-size))
       (nth [_ idx##]
         ~(read-form `byte-seq## `(p/* idx## ~byte-size)))
       (nth [_ idx## default-value#]
         (try
           ~(read-form `byte-seq## `(p/* idx## ~byte-size))
           (catch IndexOutOfBoundsException e#
             default-value#)))

       proto/InternalReduce
       (internal-reduce [_ f# start#]
         (b/byte-seq-reduce
           byte-seq## ~byte-size
           (fn [byte-seq## offset## acc#]
             (f# acc# ~(read-form `byte-seq## `offset##)))
           start#))

       proto/CollReduce
       (coll-reduce [this# f# start#]
         (byte-seq-wrapper-reduce this# f# start#))

       (coll-reduce [this# f#]
         (byte-seq-wrapper-reduce this# f#)))))

(let [;; normal read-write
      s (fn [r w]
          `[(partial list '~r)
            (partial list '~w)])

      ;; unsigned read-write
      u (fn [r w conv]
          `[(fn [b# idx#]
              (list '~conv (list '~r b# idx#)))
            (fn [b# idx# val#]
              (list '~w b# idx# (list '~conv val#)))])]
  (def ^:private primitive-types
    (->>
      [:int8    1 (s `b/get-int8 `b/put-int8)
       :uint8   1 (u `b/get-int8 `b/put-int8 `p/int8->uint8)
       :int16   2 (s `b/get-int16 `b/put-int16)
       :uint16  2 (u `b/get-int16 `b/put-int16 `p/int16->uint16)
       :int32   4 (s `b/get-int32 `b/put-int32)
       :uint32  4 (u `b/get-int32 `b/put-int32 `p/int32->uint32)
       :int64   8 (s `b/get-int64 `b/put-int64)
       :uint64  8 (u `b/get-int64 `b/put-int64 `p/int64->uint64)
       :float32 4 (s `b/get-float32 `b/put-float32)
       :float64 8 (s `b/get-float64 `b/put-float64)]
      (partition 3)
      (map
        (fn [[type size read-and-write]]
          (let [wrapper-type (symbol (str (name type) "__seq"))
                [read-form write-form] (eval read-and-write)]
            (eval
              (byte-seq-decoder-form wrapper-type size read-form))
            [type (eval
                    (unify-gensyms
                      `(let [[read-form# write-form#] ~read-and-write]
                         (reify IPrimitiveType
                           (byte-size [_#]
                             ~size)
                           (fields [_#]
                             nil)
                           (field-offset [_# _#]
                             (throw (IllegalArgumentException.)))
                           (field-type [_# _#]
                             (throw (IllegalArgumentException.)))
                           (write-to-byte-seq [_# byte-seq## offset## x##]
                             ~(write-form `byte-seq## `offset## `x##))
                           (decode-byte-seq [_# byte-seq#]
                             (new ~wrapper-type byte-seq#))
                           (read-form [_# b# idx#]
                             (read-form# b# idx#))
                           (write-form [_# b# idx# x#]
                             (write-form# b# idx# x#))))))])))
      (into {}))))


