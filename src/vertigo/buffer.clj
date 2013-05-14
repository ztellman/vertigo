(ns vertigo.buffer
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

(definterface+ IBuffer
  (get-byte ^long [_ ^long idx])
  (get-short ^long [_ ^long idx])
  (get-int ^long [_ ^long idx])
  (get-long ^long [_ ^long idx])
  (get-float ^double [_ ^long idx])
  (get-double ^double [_ ^long idx])
  
  (put-byte [_ ^long idx ^long val])
  (put-short [_ ^long idx ^long val])
  (put-int [_ ^long idx ^long val])
  (put-long [_ ^long idx ^long val])
  (put-float [_ ^long idx ^double val])
  (put-double [_ ^long idx ^double val])

  (drop-bytes [_ ^long n]))

(defmacro ^:private loop-and-get [this offset idx f]
  `(loop [chunk# ~this, idx# (long ~idx)]
     (let [next# (.next chunk#)
           next# (and next# @next#)
           idx'# (+ idx# ~offset)
           size# (.size chunk#)]
       (if (or (not next#) (< idx'# size#))
         (~f ^ByteBuffer (.buf chunk#) idx'#)
         (recur next# (- (long idx#) size#))))))

(defmacro ^:private loop-and-put [this offset idx val f]
  `(loop [chunk# ~this, idx# (long ~idx)]
     (let [next# (.next chunk#)
           next# (and next# @next#)
           idx'# (+ idx# ~offset)
           size# (.size chunk#)]
       (if (or (not next#) (< idx'# size#))
         (~f ^ByteBuffer (.buf chunk#) idx'# ~val)
         (recur next# (- (long idx#) size#))))))

(def-abstract-type ABuffer [buf offset]
  IBuffer
  (get-byte [this idx]   (long (.get buf (+ offset idx))))
  (get-short [this idx]  (long (.getShort buf (+ offset idx))))
  (get-int [this idx]    (long (.getInt buf (+ offset idx))))
  (get-long [this idx]   (.getLong buf (+ offset idx)))
  (get-float [this idx]  (double (.getFloat buf (+ offset idx))))
  (get-double [this idx] (.getDouble buf (+ offset idx)))

  (put-byte [this idx val]   (.put buf (+ offset idx) val))
  (put-short [this idx val]  (.putShort buf (+ offset idx) val))
  (put-int [this idx val]    (.putInt buf (+ offset idx) val))
  (put-long [this idx val]   (.putLong buf (+ offset idx) val))
  (put-float [this idx val]  (.putFloat buf (+ offset idx) val))
  (put-double [this idx val] (.putDouble buf (+ offset idx) val)))

(def-abstract-type ABufferChunk [buf offset size next]
  IBuffer
  (get-byte [this idx]   (long (loop-and-get this offset idx .get)))
  (get-short [this idx]  (long (loop-and-get this offset idx .getShort)))
  (get-int [this idx]    (long (loop-and-get this offset idx .getInt)))
  (get-long [this idx]   (loop-and-get this offset idx .getLong))
  (get-float [this idx]  (double (loop-and-get this offset idx .getFloat)))
  (get-double [this idx] (double (loop-and-get this offset idx .getDouble)))

  (put-byte [this idx val]   (loop-and-put this offset idx val .put))
  (put-short [this idx val]  (loop-and-put this offset idx val .putShort))
  (put-int [this idx val]    (loop-and-put this offset idx val .putInt))
  (put-long [this idx val]   (loop-and-put this offset idx val .putLong))
  (put-float [this idx val]  (loop-and-put this offset idx val .putFloat))
  (put-double [this idx val] (loop-and-put this offset idx val .putDouble)))

(deftype+ Buffer
  [^ByteBuffer buf
   ^long offset]
  ABuffer
  (drop-bytes [_ n]
    (let [idx' (+ offset n)]
      (when (<= idx' (.remaining buf))
        (Buffer. buf idx')))))

(deftype+ CloseableBuffer
  [^ByteBuffer buf
   ^long offset
   close-fn]
  ABuffer
  java.io.Closeable
  (close [_]
    (close-fn))
  (drop-bytes [_ n]
    (let [idx' (+ offset n)]
      (when (<= idx' (.remaining buf))
        (CloseableBuffer. buf idx' close-fn)))))

(defmacro ^:private loop-and-drop [this to-drop constructor]
  (unify-gensyms
    `(loop [chunk## ~this, to-drop# ~to-drop]
       (let [idx'## (+ (.offset chunk##) to-drop#)]
         (if (<= (.size chunk##) idx'##)
           (let [next# (.next chunk##)]
             (when-let [next# (and next# @next#)]
               (recur next# (- to-drop# (- (.size chunk##) (.offset chunk##))))))
           ~((eval constructor) `chunk## `idx'##))))))

(deftype+ ChunkedBuffer
  [^ByteBuffer buf
   ^long offset
   ^long size
   next]
  ABufferChunk
  (drop-bytes [this n]
    (loop-and-drop this n
      (fn [chunk idx]
        `(ChunkedBuffer. (.buf ~chunk) ~idx (.size ~chunk) (.next ~chunk))))))

(deftype+ CloseableChunkedBuffer
  [^ByteBuffer buf
   ^long offset
   ^long size
   next
   close-fn]
  ABufferChunk
  java.io.Closeable
  (close [_]
    (close-fn))
  (drop-bytes [this n]
    (loop-and-drop this n
      (fn [chunk idx]
        `(CloseableChunkedBuffer. (.buf ~chunk) ~idx (.size ~chunk) (.next ~chunk) (.close-fn ~chunk))))))

;;;

(defn chunked-buffer
  ([^ByteBuffer buf next]
     (ChunkedBuffer. (doto ^ByteBuffer buf (.order (ByteOrder/nativeOrder))) 0 (.remaining buf) next))
  ([^ByteBuffer buf next close-fn]
     (CloseableChunkedBuffer. (doto ^ByteBuffer buf (.order (ByteOrder/nativeOrder))) 0 (.remaining buf) next close-fn)))

(defn buffer
  ([buf]
     (Buffer. (doto ^ByteBuffer buf (.order (ByteOrder/nativeOrder))) 0))
  ([buf close-fn]
     (CloseableBuffer. (doto ^ByteBuffer buf (.order (ByteOrder/nativeOrder))) 0 close-fn)))

(defn array->buffer
  ([ary]
     (array->buffer ary 0 (Array/getLength ^bytes ary)))
  ([ary ^long offset ^long length]
     (let [buf (doto (ByteBuffer/allocate length)
                 (.order (ByteOrder/nativeOrder)))]
      (.put buf ary offset length)
      (.position buf 0)
      buf)))

(defn array->direct-buffer
  ([ary]
     (array->buffer ary 0 (Array/getLength ^bytes ary)))
  ([ary ^long offset ^long length]
     (let [buf (doto (ByteBuffer/allocateDirect length)
                 (.order (ByteOrder/nativeOrder)))]
       (.put buf ary offset length)
       (.position buf 0)
       buf)))

(defn input-stream->chunked-buffer
  [^InputStream input-stream ^long chunk-size]
  (let [close-fn #(.close input-stream)
        read-chunk (fn read-chunk []
                     (let [ary (Array/newInstance Byte/TYPE chunk-size)]
                       (loop [offset 0]
                         (let [len (.read input-stream ary offset (- chunk-size offset))
                               offset (+ offset len)]
                           (cond
                             (== -1 len)
                             (chunked-buffer (array->buffer ary 0 (+ offset 1)) nil close-fn)

                             (== chunk-size len)
                             (chunked-buffer (array->buffer ary 0 offset) (delay (read-chunk)) close-fn)

                             :else
                             (recur offset))))))]
    (read-chunk)))
