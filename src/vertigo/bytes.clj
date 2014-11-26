(ns vertigo.bytes
  (:use
    potemkin)
  (:require
    [byte-streams :as bytes]
    [vertigo.primitives :as prim])
  (:import
    [sun.misc
     Unsafe]
    [clojure.lang
     Compiler$LocalBinding]
    [java.nio.channels
     Channel
     Channels
     ReadableByteChannel]
    [java.nio
     DirectByteBuffer
     ByteBuffer
     ByteOrder]
    [java.io
     Closeable]
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

(definline buf-size [b]
  `(long (.remaining ~(with-meta b {:tag "java.nio.ByteBuffer"}))))

(defn slice-buffer [^ByteBuffer buf ^long offset ^long len]
  (when buf
    (let [order (.order buf)]
      (-> buf
        .duplicate
        (.position offset)
        ^ByteBuffer (.limit (+ offset len))
        .slice
        (.order order)))))

;;;

(deftype+ ByteSeq
  [^ByteBuffer buf
   close-fn
   flush-fn]

  IByteSeq
  (get-int8 [this idx]     (long (.get buf idx)))
  (get-int16 [this idx]    (long (.getShort buf idx)))
  (get-int32 [this idx]    (long (.getInt buf idx)))
  (get-int64 [this idx]    (.getLong buf idx))
  (get-float32 [this idx]  (double (.getFloat buf idx)))
  (get-float64 [this idx]  (.getDouble buf idx))

  (put-int8 [this idx val]     (.put buf idx (byte val)))
  (put-int16 [this idx val]    (.putShort buf idx (short val)))
  (put-int32 [this idx val]    (.putInt buf idx (int val)))
  (put-int64 [this idx val]    (.putLong buf idx val))
  (put-float32 [this idx val]  (.putFloat buf idx (float val)))
  (put-float64 [this idx val]  (.putDouble buf idx val))

  (byte-order [_] (.order buf))
  (set-byte-order! [this order] (.order buf order) this)

  java.io.Closeable
  (close [this]
    (when close-fn
      (close-fn this)))

  (close-fn [_] close-fn)
  (flush-fn [_] flush-fn)

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

  (byte-count [_]
    (buf-size buf))

  (drop-bytes [this n]
    (when (< n (buf-size buf))
      (slice this n (- (buf-size buf) n))))

  (slice [_ offset len]
    (ByteSeq. (slice-buffer buf offset len) close-fn flush-fn)))

;;;

(deftype+ UnsafeByteSeq
  [^Unsafe unsafe
   ^ByteBuffer buf
   ^long loc
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
  (get-int8 [this idx]     (long (.getByte unsafe (+ loc idx))))
  (get-int16 [this idx]    (long (.getShort unsafe (+ loc idx))))
  (get-int32 [this idx]    (long (.getInt unsafe (+ loc idx))))
  (get-int64 [this idx]    (.getLong unsafe (+ loc idx)))
  (get-float32 [this idx]  (double (.getFloat unsafe (+ loc idx))))
  (get-float64 [this idx]  (.getDouble unsafe (+ loc idx)))

  (put-int8 [this idx val]     (.putByte unsafe (+ loc idx) (byte val)))
  (put-int16 [this idx val]    (.putShort unsafe (+ loc idx) (short val)))
  (put-int32 [this idx val]    (.putInt unsafe (+ loc idx) (int val)))
  (put-int64 [this idx val]    (.putLong unsafe (+ loc idx) val))
  (put-float32 [this idx val]  (.putFloat unsafe (+ loc idx) (float val)))
  (put-float64 [this idx val]  (.putDouble unsafe (+ loc idx) val))

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

  (byte-count [_]
    (buf-size buf))

  (drop-bytes [this n]
    (when (< n (buf-size buf))
      (slice this n (- (buf-size buf) n))))

  (slice [_ offset len]
    (ByteSeq. (slice-buffer buf offset len) close-fn flush-fn)))

;;;

(defmacro ^:private doto-nth
  [this buf size idx f & rest]
  `(try
     (loop [chunk# ~this, idx# (long ~idx)]
       (let [buf-size# (.chunk-size chunk#)]
         (if (< idx# buf-size#)
           (let [^ByteBuffer buf# (.buf chunk#)]
             (~f buf# idx# ~@rest))
           (let [next# (.next-chunk chunk#)
                 next# (when-not (nil? next#) @next#)
                 next# (when-not (nil? next#) (set-byte-order! next# (byte-order chunk#)))]
             (recur next# (- idx# buf-size#))))))
     (catch NullPointerException e#
       (throw (IndexOutOfBoundsException. (str ~idx))))))

(deftype ChunkedByteSeq
  [^ByteBuffer buf
   ^long chunk-size
   next-chunk
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
    (ChunkedByteSeq. buffer (buf-size buffer) (delay this) close-fn flush-fn))

  IByteSeq

  (get-int8 [this idx]     (long (doto-nth this buf chunk-size idx .get)))
  (get-int16 [this idx]    (long (doto-nth this buf chunk-size idx .getShort)))
  (get-int32 [this idx]    (long (doto-nth this buf chunk-size idx .getInt)))
  (get-int64 [this idx]    (doto-nth this buf chunk-size idx .getLong))
  (get-float32 [this idx]  (double (doto-nth this buf chunk-size idx .getFloat)))
  (get-float64 [this idx]  (doto-nth this buf chunk-size idx .getDouble))

  (put-int8 [this idx val]     (doto-nth this buf chunk-size idx .put (byte val)))
  (put-int16 [this idx val]    (doto-nth this buf chunk-size idx .putShort (short val)))
  (put-int32 [this idx val]    (doto-nth this buf chunk-size idx .putInt (int val)))
  (put-int64 [this idx val]    (doto-nth this buf chunk-size idx .putLong val))
  (put-float32 [this idx val]  (doto-nth this buf chunk-size idx .putFloat (float val)))
  (put-float64 [this idx val]  (doto-nth this buf chunk-size idx .putDouble val))

  (byte-order [_] (.order buf))
  (set-byte-order! [this order] (.order buf order) this)

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
      (let [size (buf-size (.buf byte-seq))]
        (if (< size (+ offset len))
          (ChunkedByteSeq.
            (slice-buffer buf offset (- size offset))
            (- size offset)
            (delay (slice @next-chunk 0 (- len size)))
            close-fn
            flush-fn)
          (ChunkedByteSeq.
            (slice-buffer buf offset len)
            len
            nil
            close-fn
            flush-fn)))))

  (drop-bytes [this n]
    (loop [chunk this, to-drop n]
      (when-not (nil? chunk)
        (let [buf (.buf chunk)
              size (buf-size buf)]
          (if (<= size to-drop)
            (recur (next chunk) (- to-drop size))
            (ChunkedByteSeq.
              (slice-buffer buf to-drop (- size to-drop))
              (- size to-drop)
              (.next-chunk chunk)
              (.close-fn chunk)
              (.flush-fn chunk))))))))

(defmethod print-method ChunkedByteSeq [o ^java.io.Writer writer]
  (print-method (map identity o) writer))

;;;

(def ^{:doc "If true, disables all runtime index bounds checking."}
  use-unsafe?
  (boolean (System/getProperty "vertigo.unsafe")))

(def ^Unsafe ^:private unsafe
  (when use-unsafe?
    (let [f (.getDeclaredField Unsafe "theUnsafe")]
      (.setAccessible f true)
      (.get f nil))))

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

(defn chunked-byte-seq
  ([buf next]
     (chunked-byte-seq buf next nil nil))
  ([buf next close-fn flush-fn]
     (ChunkedByteSeq.
       (with-native-order (or buf (buffer 0)))
       (buf-size buf)
       next
       close-fn
       flush-fn)))

(defn byte-seq
  "Wraps a byte-buffer inside a byte-seq."
  ([buf]
     (byte-seq buf nil nil))
  ([^ByteBuffer buf close-fn flush-fn]
     (if (and (.isDirect buf) use-unsafe?)
       (UnsafeByteSeq. unsafe (with-native-order buf) (.address ^DirectByteBuffer buf) close-fn flush-fn)
       (ByteSeq. (with-native-order buf) close-fn flush-fn))))

(defn cross-section
  "Returns a chunked-byte-seq representing a sequence of slices, starting at `offset`, `length`
   bytes long, and separated by `stride` bytes."
  [byte-seq ^long offset ^long length ^long stride]
  (when-let [buf (and byte-seq
                   (-> byte-seq
                     bytes/to-byte-buffers
                     first
                     (slice-buffer offset length)))]
    (chunked-byte-seq
      buf
      (-> byte-seq
        (drop-bytes (+ offset stride))
        (cross-section 0 length stride)
        delay)
      (close-fn byte-seq)
      (flush-fn byte-seq))))

(defn to-byte-seq
  "Converts `x` to a byte-seq."
  ([x]
     (to-byte-seq x nil))
  ([x options]
     (bytes/convert x ByteSeq options)))

(defn to-chunked-byte-seq
  "Converts `x` to a chunked-byte-seq."
  ([x]
     (to-chunked-byte-seq x nil))
  ([x options]
     (bytes/convert x ChunkedByteSeq options)))

;;;

(bytes/def-conversion [ByteSeq (bytes/seq-of ByteBuffer)]
  [byte-seq options]
  (if-let [chunk-size (:chunk-size options)]
    (bytes/to-byte-buffers (.buf byte-seq) options)
    [(.duplicate ^ByteBuffer (.buf byte-seq))]))

(bytes/def-conversion [UnsafeByteSeq (bytes/seq-of ByteBuffer)]
  [byte-seq options]
  (if-let [chunk-size (:chunk-size options)]
    (bytes/to-byte-buffers (.buf byte-seq) options)
    [(.duplicate ^ByteBuffer (.buf byte-seq))]))

(bytes/def-conversion [ChunkedByteSeq (bytes/seq-of ByteBuffer)]
  [byte-seq options]
  (let [bufs (lazy-seq
               (cons
                 (.duplicate ^ByteBuffer (.buf byte-seq))
                 (when-let [s (seq (next byte-seq))]
                   (bytes/to-byte-buffers s))))]
    (if-let [chunk-size (:chunk-size options)]
      (-> bufs bytes/to-readable-channel (bytes/to-byte-buffers options))
      bufs)))

(bytes/def-conversion [(bytes/seq-of ByteBuffer) ChunkedByteSeq]
  [bufs]
  (let [buf (first bufs)]
    (chunked-byte-seq
      (first bufs)
      (delay (bytes/convert (rest bufs) ChunkedByteSeq))
      (when (instance? Closeable bufs)
        #(.close ^Closeable bufs))
      nil)))

(bytes/def-conversion [ByteBuffer ByteSeq]
  [buf]
  (byte-seq buf))
