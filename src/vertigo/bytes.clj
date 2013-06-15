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

  (byte-count ^long [_]
    "Returns the number of bytes in the byte-seq.")
  (drop-bytes [_ ^long n]
    "Returns a new byte-seq without the first `n` bytes.")
  (slice [_ ^long offset ^long len]
    "Returns a subset of the byte-seq, starting at `offset` bytes, and `len` bytes long.")

  (unwrap-buffers [_]
    "Returns a sequence of the underlying bytes.")

  (close-fn [_]
    "Returns the function, if it exists, that closes the underlying source of bytes.")
  (flush-fn [_]
    "Returns the function, if it exists, that flushes the underlying source of bytes.")
  
  (byte-order [_]
    "Returns the java.nio.ByteOrder of the underlying buffer.")
  (set-byte-order! [_ order]
    "Sets the byte order of the underlying buffer, and returns the byte-seq.")
  (byte-seq-reduce [_ stride read-fn f start]
    "A byte-seq specific version of the CollReduce protocol."))

(definline big-endian?
  "Returns true if the byte-seq is big-endian."
  [byte-seq]
  `(= ByteOrder/BIG_ENDIAN (byte-order ~byte-seq)))

(definline little-endian?
  "Returns true if the byte-seq is little-endian."
  [byte-seq]
  `(= ByteOrder/LITTLE_ENDIAN (byte-order ~byte-seq)))

;;;

(definline buf-size [b]
  `(long (.remaining ~(with-meta b {:tag "java.nio.ByteBuffer"}))))

(defn slice-buffer [^ByteBuffer buf ^long offset ^long len]
  (let [order (.order buf)]
    (-> buf
      .duplicate
      (.position offset)
      ^ByteBuffer (.limit (+ offset len))
      .slice
      (.order order))))

(deftype+ ByteSeq
  [^ByteBuffer buf
   close-fn
   flush-fn]

  java.io.Closeable
  (close [this]
    (when close-fn
      (close-fn this)))

  (close-fn [_]
    close-fn)
  (flush-fn [_]
    flush-fn)
  
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
          size (buf-size buf)]
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
    (buf-size buf))

  (drop-bytes [this n]
    (when (< n (buf-size buf))
      (slice this n (- (buf-size buf) n))))

  (slice [_ offset len]
    (ByteSeq. (slice-buffer buf offset len) close-fn flush-fn))) 

;;;

(defmacro ^:private doto-nth
  [this buf idx f & rest]
  `(try
     (loop [chunk# ~this, idx# (long ~idx)]
       (let [^ByteBuffer buf# (.buf chunk#)
             buf-size# (buf-size buf#)]
         (if (< idx# buf-size#)
           (~f buf# idx# ~@rest)
           (let [next# (.next-chunk chunk#)
                 next# (when-not (nil? next#) @next#)
                 next# (when-not (nil? next#) (set-byte-order! next# (byte-order chunk#)))]
             (recur next# (- idx# buf-size#))))))
     (catch NullPointerException e#
       (throw (IndexOutOfBoundsException. (str ~idx))))))

(deftype+ ChunkedByteSeq
  [^ByteBuffer buf
   next-chunk
   close-fn
   flush-fn]

  java.io.Closeable
  (close [_]
    (when close-fn
      (close-fn)))

  (close-fn [_]
    close-fn)
  (flush-fn [_]
    flush-fn)

  clojure.lang.ISeq
  (seq [this]
    this)
  (first [_]
    (ByteSeq. buf close-fn flush-fn))
  (next [this]
    (let [nxt (when-not (nil? next-chunk) @next-chunk)]
      (when-not (nil? nxt)
        (set-byte-order! nxt (.order buf)))
      nxt))
  (more [this]
    (or (next this) '()))
  (cons [this buffer]
    (ChunkedByteSeq. buf (delay this) close-fn flush-fn))

  IByteSeq
  
  (get-int8 [this idx]     (long (doto-nth this buf idx .get)))
  (get-int16 [this idx]    (long (doto-nth this buf idx .getShort)))
  (get-int32 [this idx]    (long (doto-nth this buf idx .getInt)))
  (get-int64 [this idx]    (doto-nth this buf idx .getLong))
  (get-float32 [this idx]  (double (doto-nth this buf idx .getFloat)))
  (get-float64 [this idx]  (doto-nth this buf idx .getDouble))

  (put-int8 [this idx val]     (doto-nth this buf idx .put val))
  (put-int16 [this idx val]    (doto-nth this buf idx .putShort val))
  (put-int32 [this idx val]    (doto-nth this buf idx .putInt val))
  (put-int64 [this idx val]    (doto-nth this buf idx .putLong val))
  (put-float32 [this idx val]  (doto-nth this buf idx .putFloat val))
  (put-float64 [this idx val]  (doto-nth this buf idx .putDouble val))

  (byte-order [_] (.order buf))
  (set-byte-order! [this order] (.order buf order) this)

  (unwrap-buffers [this]
    (cons
      buf
      (lazy-seq
        (when-let [nxt (next this)]
          (unwrap-buffers nxt)))))

  (byte-seq-reduce [this stride read-fn f start]
    (let [stride (long stride)]
      (loop [chunk this, val start]
        (let [val' (let [size (buf-size (.buf chunk))]
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
        (recur (next chunk) (+ acc (long (.remaining ^ByteBuffer (.buf chunk))))))))

  (slice [this offset len]
    (when-let [^ChunkedByteSeq byte-seq (drop-bytes this offset)]
      (let [buf-size (buf-size (.buf byte-seq))]
        (if (< buf-size (+ offset len))
          (ChunkedByteSeq.
            (slice-buffer buf offset (- buf-size offset))
            (delay (slice @next-chunk 0 (- len buf-size)))
            close-fn
            flush-fn)
          (ChunkedByteSeq.
            (slice-buffer buf offset len)
            nil
            close-fn
            flush-fn)))))
  
  (drop-bytes [this n]
    (loop [chunk this, to-drop n]
      (when-not (nil? chunk)
        (let [size (buf-size (.buf chunk))]
          (if (<= size to-drop)
            (recur (next chunk) (- to-drop size))
            (ChunkedByteSeq.
              (slice-buffer (.buf chunk) to-drop (- size to-drop))
              (.next-chunk chunk)
              (.close-fn chunk)
              (.flush-fn chunk))))))))

(defmethod print-method ChunkedByteSeq [o ^java.io.Writer writer]
  (print-method (map identity o) writer))

;;;

(defn- with-native-order [^ByteBuffer buf]
  (.order buf (ByteOrder/nativeOrder)))

(defn buffer
  "Creates a byte-buffer."
  [^long size]
  (with-native-order (ByteBuffer/allocate size)))

(defn direct-buffer
  "Creates a direct byte-buffer."
  [^long size]
  (with-native-order (ByteBuffer/allocateDirect size)))

(defn lazy-byte-seq
  "Returns a lazily realized byte sequence, based on an initial `buf`, and a promise containing the
   next byte-buffer."
  ([^ByteBuffer buf next]
     (lazy-byte-seq buf next nil nil))
  ([^ByteBuffer buf next close-fn flush-fn]
     (ChunkedByteSeq.
       (with-native-order (or buf (buffer 0)))
       next
       close-fn
       flush-fn)))

(defn cross-section
  "Returns a chunked-byte-seq representing a sequence of slices, starting at `offset`, `length`
   bytes long, and separated by `stride` bytes."
  [byte-seq ^long offset ^long stride ^long length]
  (lazy-byte-seq
    (slice-buffer byte-seq offset length)
    (-> byte-seq (drop-bytes (+ offset length stride)) (cross-section 0 stride length) delay)
    (close-fn byte-seq)))

(defn byte-seq
  "Wraps a byte-buffer inside a byte-seq."
  ([buf]
     (byte-seq buf nil nil))
  ([^ByteBuffer buf close-fn flush-fn]
     (ByteSeq. (with-native-order buf) close-fn flush-fn)))

(defn array->buffer
  "Converts a byte-array to a byte-buffer."
  ([ary]
     (array->buffer ary 0 (Array/getLength ^bytes ary)))
  ([ary ^long offset ^long length]
     (with-native-order (ByteBuffer/wrap ary offset length))))

(defn array->direct-buffer
  "Converts a byte-array to a direct buffer."
  ([ary]
     (array->direct-buffer ary 0 (Array/getLength ^bytes ary)))
  ([ary ^long offset ^long length]
     (let [^ByteBuffer buf (direct-buffer length)]
       (.put buf ary offset length)
       (.position buf 0)
       buf)))

(defn buffer->array
  "Converts a byte-buffer to a byte-array."
  [^ByteBuffer buf]
  (if (.hasArray buf)
    (.array buf)
    (let [^bytes ary (Array/newInstance Byte/TYPE (.remaining buf))]
      (doto buf .mark (.get ary) .reset)
      ary)))

(defn input-stream->byte-seq
  "Converts an input-stream to a chunked byte-seq.  The chunk size must be a multiple of the size of the
   inner type."
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
                                (lazy-byte-seq (->buffer ary 0 (+ offset 1)) nil close-fn nil)

                                (== chunk-size len)
                                (lazy-byte-seq (->buffer ary 0 offset) (delay (read-chunk)) close-fn nil)

                                :else
                                (recur offset))))))]
       (read-chunk))))
