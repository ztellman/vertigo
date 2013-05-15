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

  (drop-bytes [_ ^long n])

  (byte-order [_])
  (set-byte-order! [_ order]))

(deftype+ ByteSeq
  [^ByteBuffer buf
   ^long offset]
   IByteSeq
  (get-byte [this idx]   (long (.get buf (+ offset idx))))
  (get-short [this idx]  (long (.getShort buf (+ offset idx))))
  (get-int [this idx]    (long (.getInt buf (+ offset idx))))
  (get-long [this idx]   (.getLong buf (+ offset idx)))
  (get-float [this idx]  (double (.getFloat buf (+ offset idx))))
  (get-double [this idx] (.getDouble buf (+ offset idx)))

  (put-byte [this idx val]   (.put buf (+ offset idx) val) nil)
  (put-short [this idx val]  (.putShort buf (+ offset idx) val) nil)
  (put-int [this idx val]    (.putInt buf (+ offset idx) val) nil)
  (put-long [this idx val]   (.putLong buf (+ offset idx) val) nil)
  (put-float [this idx val]  (.putFloat buf (+ offset idx) val) nil)
  (put-double [this idx val] (.putDouble buf (+ offset idx) val) nil)

  (byte-order [_] (.order buf))
  (set-byte-order! [this order] (.order buf order) this)

  (drop-bytes [_ n]
    (let [idx' (+ offset n)]
      (when (<= idx' (.remaining buf))
        (ByteSeq. buf idx'))))
  
  clojure.lang.Counted
  (count [_] (- (.remaining buf) offset)))

;;;

(defmacro ^:private loop-and-get [this offset idx f]
  `(loop [chunk# ~this, idx# ~idx]
     (if (nil? chunk#)
       (throw (IndexOutOfBoundsException. (str ~idx)))
       (let [idx'# (+ idx# ~offset)
             size# (.size chunk#)]
         (if (< idx'# size#)
           (~f ^ByteBuffer (.buf chunk#) idx'#)
           (let [next# (.next chunk#)
                 next# (when-not (nil? next#) @next#)
                 next# (when-not (nil? next#) (set-byte-order! next# (byte-order chunk#)))]
             (recur next# (- (long idx#) size#))))))))

(defmacro ^:private loop-and-put [this offset idx val f]
  `(loop [chunk# ~this, idx# ~idx]
     (if (nil? chunk#)
       (throw (IndexOutOfBoundsException. (str ~idx)))
       (let [idx'# (+ idx# ~offset)
             size# (.size chunk#)]
         (if (< idx'# size#)
           (~f ^ByteBuffer (.buf chunk#) idx'# ~val)
           (let [next# (.next chunk#)
                 next# (when-not (nil? next#) @next#)
                 next# (when-not (nil? next#) (set-byte-order! next# (byte-order chunk#)))]
             (recur next# (- (long idx#) size#))))))))

(defmacro ^:private loop-and-drop [this to-drop constructor]
  (unify-gensyms
    `(loop [chunk## ~this, to-drop# ~to-drop]
       (when-not (nil? chunk##)
         (let [idx'## (+ (.offset chunk##) to-drop#)]
          (if (<= (.size chunk##) idx'##)
            (let [next# (.next chunk##)]
              (let [next# (.next chunk##)
                    next# (when-not (nil? next#) @next#)
                    next# (when-not (nil? next#) (set-byte-order! next# (byte-order chunk##)))]
                (recur next# (- to-drop# (- (.size chunk##) (.offset chunk##))))))
            ~((eval constructor) `chunk## `idx'##)))))))

(deftype+ ChunkedByteSeq
  [^ByteBuffer buf
   ^long offset
   ^long size
   next
   close-fn]

  IByteSeq
  
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
  (put-double [this idx val] (loop-and-put this offset idx val .putDouble))

  (byte-order [_] (.order buf))
  (set-byte-order! [this order] (.order buf order) this)

  java.io.Closeable
  (close [_]
    (when close-fn
      (close-fn)))
  (drop-bytes [this n]
    (loop-and-drop this n
      (fn [chunk idx]
        `(ChunkedByteSeq. (.buf ~chunk) ~idx (.size ~chunk) (.next ~chunk) (.close-fn ~chunk))))))

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
  (ByteSeq. (with-native-order buf) 0))

(defn array->buffer
  ([ary]
     (array->buffer ary 0 (Array/getLength ^bytes ary)))
  ([ary ^long offset ^long length]
     (with-native-order (ByteBuffer/wrap ary offset length))))

(defn array->direct-buffer
  ([ary]
     (array->buffer ary 0 (Array/getLength ^bytes ary)))
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
