(ns vertigo.bytes
  (:use
    potemkin)
  (:require
    [vertigo.primitives :as prim])
  (:import
    [java.io
     InputStream]
    [java.nio
     ByteBuffer
     ByteOrder]
    [vertigo.utils
     Primitives]
    [java.util.concurrent.atomic
     AtomicBoolean]
    [java.lang.reflect
     Array]))

;;;

(prim/use-primitive-operators)

(definterface+ IByteSeq
  (get-int8 ^long [_ ^long idx])
  (get-int16 ^long [_ ^long idx])
  (get-int32 ^long [_ ^long idx])
  (get-int64 ^long [_ ^long idx])
  (get-float32 ^double [_ ^long idx])
  (get-float64 ^double [_ ^long idx])
  
  (put-int8 [_ ^long idx ^long val])
  (put-int16 [_ ^long idx ^long val])
  (put-int32 [_ ^long idx ^long val])
  (put-int64 [_ ^long idx ^long val])
  (put-float32 [_ ^long idx ^double val])
  (put-float64 [_ ^long idx ^double val])

  (byte-count ^long [_])
  (drop-bytes [_ ^long n])
  (slice [_ ^long offset ^long len])
  (unwrap-buffers [_])

  (byte-order [_])
  (set-byte-order! [_ order])

  (byte-seq-reduce [_ stride read-fn f start]))

(defn big-endian?
  {:inline (fn [byte-seq] `(= java.nio.ByteOrder/BIG_ENDIAN (vertigo.bytes/byte-order ~byte-seq)))}
  [byte-seq]
  (= ByteOrder/BIG_ENDIAN (byte-order byte-seq)))

(defn little-endian?
  {:inline (fn [byte-seq] `(= java.nio.ByteOrder/LITTLE_ENDIAN (vertigo.bytes/byte-order ~byte-seq)))}
  [byte-seq]
  (= ByteOrder/LITTLE_ENDIAN (byte-order byte-seq)))

;;;

(defn slice-buffer [^ByteBuffer buf ^long offset ^long len]
  (let [order (.order buf)]
    (-> buf .duplicate (.position offset) ^ByteBuffer (.limit (+ offset len)) .slice (.order order))))

(deftype+ ByteSeq
  [^ByteBuffer buf]
  IByteSeq
  (get-int8 [this idx]     (long (.get buf idx)))
  (get-int16 [this idx]    (long (.getShort buf idx)))
  (get-int32 [this idx]    (long (.getInt buf idx)))
  (get-int64 [this idx]    (.getLong buf idx))
  (get-float32 [this idx]  (double (.getFloat buf idx)))
  (get-float64 [this idx]  (.getDouble buf idx))

  (put-int8 [this idx val]     (.put buf idx val))
  (put-int16 [this idx val]    (.putShort buf idx val))
  (put-int32 [this idx val]    (.putInt buf idx val))
  (put-int64 [this idx val]    (.putLong buf idx val))
  (put-float32 [this idx val]  (.putFloat buf idx val))
  (put-float64 [this idx val]  (.putDouble buf idx val))

  (byte-order [_] (.order buf))
  (set-byte-order! [this order] (.order buf order) this)

 (byte-seq-reduce [this stride read-fn f start]
    (let [stride (long stride)
          size (.remaining buf)]
      (loop [idx 0, val start]
        (if (<= size idx)
          val
          (let [val' (f val (read-fn this idx))]
            (if (reduced? val')
              @val'
              (recur (+ idx stride) val')))))))

  (unwrap-buffers [_]
    [buf])

  (byte-count [_]
    (.remaining buf))

  (drop-bytes [this n]
    (when (< n (.remaining buf))
      (slice this n (- (.remaining buf) n))))

  (slice [_ offset len]
    (ByteSeq. (slice-buffer buf offset len))))

;;;

(defmacro ^:private next-chunk [chunk]
  `(let [next# (.next-chunk ~chunk)
         next# (when-not (nil? next#) @next#)
         next# (when-not (nil? next#) (set-byte-order! next# (byte-order ~chunk)))]
     next#))

(defmacro ^:private find-in-chunks
  "Finds the appropriate "
  [this idx f & rest] 
  `(loop [chunk# ~this, idx# (long ~idx)]
     (if (nil? chunk#)
       (throw (IndexOutOfBoundsException. (str ~idx)))
       (let [idx'# (+ idx# (.offset chunk#))
             limit# (.limit chunk#)]
         (if (< idx'# limit#)
           (~f ^ByteBuffer (.buf chunk#) (int idx'#) ~@rest)
           (recur (next-chunk chunk#) (- idx# limit#)))))))

(deftype+ ChunkedByteSeq
  [^ByteBuffer buf
   ^long offset
   ^long limit
   next-chunk
   close-fn]

  clojure.lang.ISeq
  (seq [this]
    this)
  (first [_]
    (ByteSeq. (slice-buffer buf offset (- limit offset))))
  (next [this]
    (let [nxt (when-not (nil? next-chunk) @next-chunk)]
      (when-not (nil? nxt)
        (set-byte-order! nxt (.order buf)))
      nxt))
  (more [this]
    (or (next this) '()))
  (cons [this buffer]
    (ChunkedByteSeq. buf 0 (.remaining buf) (delay this) close-fn))

  IByteSeq
  
  (get-int8 [this idx]     (long (find-in-chunks this idx .get)))
  (get-int16 [this idx]    (long (find-in-chunks this idx .getShort)))
  (get-int32 [this idx]    (long (find-in-chunks this idx .getInt)))
  (get-int64 [this idx]    (find-in-chunks this idx .getLong))
  (get-float32 [this idx]  (double (find-in-chunks this idx .getFloat)))
  (get-float64 [this idx]  (find-in-chunks this idx .getDouble))

  (put-int8 [this idx val]     (find-in-chunks this idx .put val))
  (put-int16 [this idx val]    (find-in-chunks this idx .putShort val))
  (put-int32 [this idx val]    (find-in-chunks this idx .putInt val))
  (put-int64 [this idx val]    (find-in-chunks this idx .putLong val))
  (put-float32 [this idx val]  (find-in-chunks this idx .putFloat val))
  (put-float64 [this idx val]  (find-in-chunks this idx .putDouble val))

  (byte-order [_] (.order buf))
  (set-byte-order! [this order] (.order buf order) this)

  (unwrap-buffers [this]
    (cons
      (-> buf .duplicate (.position offset) ^ByteBuffer (.limit limit) .slice)
      (lazy-seq
        (when-let [nxt (next this)]
          (unwrap-buffers nxt)))))

  (byte-seq-reduce [this stride read-fn f start]
    (let [stride (long stride)]
      (loop [chunk this, val start]
        (let [val' (let [size (- (.limit chunk) (.offset chunk))]
                     (loop [idx 0, val val]
                       (if (<= size idx)
                         val
                         (let [val' (f val (read-fn chunk idx))]
                           (if (reduced? val')
                             val'
                             (recur (+ idx stride) val'))))))]
          (if (reduced? val')
            @val'
            (let [nxt (next chunk)]
              (if (nil? nxt)
                val'
                (recur nxt val'))))))))

  (byte-count [this]
    (loop [chunk this, acc 0]
      (if (nil? chunk)
        acc
        (recur (next chunk) (+ acc (- (.limit chunk) (.offset chunk)))))))

  (slice [_ offset' len]
    (when (< limit (+ offset offset' len))
      (throw (IllegalArgumentException. "slice length must be less than or equal to the size of the byte-seq chunk")))
    (ChunkedByteSeq. buf (+ offset offset') (+ offset offset' len) next-chunk close-fn))
  
  (drop-bytes [this n]
    (loop [chunk this, to-drop n]
      (when-not (nil? chunk)
        (let [idx' (+ (.offset chunk) to-drop)
              limit (.limit chunk)]
          (if (<= limit idx')
            (recur (next chunk) (- to-drop (- limit (.offset chunk))))
            (ChunkedByteSeq. (.buf chunk) idx' limit (.next-chunk chunk) (.close-fn chunk)))))))

  java.io.Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method ChunkedByteSeq [o ^java.io.Writer writer]
  (print-method (map identity o) writer))

;;;

(defn- with-native-order [^ByteBuffer buf]
  (.order buf (ByteOrder/nativeOrder)))

(defn lazy-byte-seq
  "Returns a lazily realized byte sequence, based on an initial `buf`, and a promise containing the
   next byte-buffer."
  ([^ByteBuffer buf next]
     (lazy-byte-seq buf next nil))
  ([^ByteBuffer buf next close-fn]
     (ChunkedByteSeq.
       (with-native-order (or buf (ByteBuffer/allocate 0)))
       0
       (if buf (.remaining buf) 0)
       next
       close-fn)))

(defn byte-seq
  [^ByteBuffer buf]
  (ByteSeq. (with-native-order buf)))

(defn array->buffer
  ([ary]
     (array->buffer ary 0 (Array/getLength ^bytes ary)))
  ([ary ^long offset ^long length]
     (with-native-order (ByteBuffer/wrap ary offset length))))

(defn array->direct-buffer
  ([ary]
     (array->direct-buffer ary 0 (Array/getLength ^bytes ary)))
  ([ary ^long offset ^long length]
     (let [^ByteBuffer buf (with-native-order (ByteBuffer/allocateDirect length))]
       (.put buf ary offset length)
       (.position buf 0)
       buf)))

(defn buffer->byte-array
  [^ByteBuffer buf]
  (if (.hasArray buf)
    (.array buf)
    (let [^bytes ary (Array/newInstance Byte/TYPE (.remaining buf))]
      (doto buf .mark (.get ary) .reset)
      ary)))

(defn input-stream->byte-seq
  ([input-stream chunk-size]
     (input-stream->byte-seq input-stream chunk-size false))
  ([^InputStream input-stream ^long chunk-size direct?]
     (let [close-fn #(.close input-stream)
           read-chunk (fn read-chunk []
                        (let [ary (Array/newInstance Byte/TYPE chunk-size)
                              ->buffer (if direct?
                                         array->direct-buffer
                                         array->buffer)]
                          (loop [offset 0]
                            (let [len (.read input-stream ary offset (- chunk-size offset))
                                  offset (+ offset len)]
                              (cond
                                (== -1 len)
                                (lazy-byte-seq (->buffer ary 0 (+ offset 1)) nil close-fn)

                                (== chunk-size len)
                                (lazy-byte-seq (->buffer ary 0 offset) (delay (read-chunk)) close-fn)

                                :else
                                (recur offset))))))]
       (read-chunk))))
