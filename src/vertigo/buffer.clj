(ns vertigo.buffer
  (:use
    potemkin)
  (:require
    [vertigo.primitives :as prim])
  (:import
    [java.io
     InputStream]
    [java.nio
     ByteBuffer]
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
  
  (put-byte ^void [_ ^long idx ^long val])
  (put-short ^void [_ ^long idx ^long val])
  (put-int ^void [_ ^long idx ^long val])
  (put-long ^void [_ ^long idx ^long val])
  (put-float ^void [_ ^long idx ^double val])
  (put-double ^void [_ ^long idx ^double val]))

(defmacro ^:private loop-and-get [this idx f]
  `(loop [chunk# ~this, idx# (long ~idx)]
     (let [next# (.next chunk#)
           next# (and next# @next#)]
       (if (or (not next#) (< idx# (.size chunk#)))
         (~f (.buf chunk#) idx#)
         (recur next#
           (- (long idx#) (.size chunk#)))))))

(defmacro ^:private loop-and-put [this idx val f]
  `(loop [chunk# ~this, idx# (long ~idx)]
     (let [next# (.next chunk#)
           next# (and next# @next#)]
       (if (or (not next#) (< idx# (.size chunk#)))
         (~f (.buf chunk#) idx# ~val)
         (recur next#
           (- (long idx#) (.size chunk#)))))))

(def-abstract-type ABuffer [buf]
  IBuffer
  (get-byte [this idx]   (long (.get buf idx)))
  (get-short [this idx]  (long (.getShort buf idx)))
  (get-int [this idx]    (long (.getInt buf idx)))
  (get-long [this idx]   (.getLong buf idx))
  (get-float [this idx]  (double (.getFloat buf idx)))
  (get-double [this idx] (.getDouble buf idx))

  (put-byte [this idx val]   (.put buf idx val))
  (put-short [this idx val]  (.putShort buf idx val))
  (put-int [this idx val]    (.putInt buf idx val))
  (put-long [this idx val]   (.putLong buf idx val))
  (put-float [this idx val]  (.putFloat buf idx val))
  (put-double [this idx val] (.putDouble buf idx val)))

(def-abstract-type ABufferChunk [buf size next]
  IBuffer
  (get-byte [this idx]   (loop-and-get this idx .get))
  (get-short [this idx]  (loop-and-get this idx .getShort))
  (get-int [this idx]    (loop-and-get this idx .getInt))
  (get-long [this idx]   (loop-and-get this idx .getLong))
  (get-float [this idx]  (loop-and-get this idx .getFloat))
  (get-double [this idx] (loop-and-get this idx .getDouble))

  (put-byte [this idx val]   (loop-and-put this idx val .put))
  (put-short [this idx val]  (loop-and-put this idx val .putShort))
  (put-int [this idx val]    (loop-and-put this idx val .putInt))
  (put-long [this idx val]   (loop-and-put this idx val .putLong))
  (put-float [this idx val]  (loop-and-put this idx val .putFloat))
  (put-double [this idx val] (loop-and-put this idx val .putDouble))

  clojure.lang.ISeq
  (first [this] this)
  (next [_] (when next @next))
  (more [this]
    (if-let [n (next this)]
      n
      '()))

  clojure.lang.Seqable
  (seq [this] this))

(deftype+ Buffer
  [^ByteBuffer buf]
  ABuffer)

(deftype+ CloseableBuffer
  [^ByteBuffer buf
   ^AtomicBoolean latch
   close-fn]
  ABuffer
  java.io.Closeable
  (close [_]
    (when (.compareAndSet latch false true)
      (close-fn))))

(deftype+ BufferChunk
  [^ByteBuffer buf
   ^long size
   next]
  ABufferChunk)

(deftype+ CloseableBufferChunk
  [^ByteBuffer buf
   ^long size
   next
   ^AtomicBoolean latch
   close-fn]
  ABufferChunk
  java.io.Closeable
  (close [_]
    (when (.compareAndSet latch false true)
      (close-fn))))

;;;

(defn chunked-buffer
  ([^ByteBuffer buf next]
     (BufferChunk. buf (.remaining buf) next))
  ([^ByteBuffer buf next close-fn]
     (CloseableBufferChunk. buf (.remaining buf) next (AtomicBoolean. false) close-fn)))

(defn buffer
  ([^ByteBuffer buf]
     (Buffer. buf))
  ([^ByteBuffer buf close-fn]
     (CloseableBuffer. buf (AtomicBoolean. false) close-fn)))

(defn array->buffer
  (^ByteBuffer [ary]
     (array->buffer ary 0 (Array/getLength ^bytes ary)))
  (^ByteBuffer [ary ^long offset ^long length]
    (let [buf (ByteBuffer/allocate length)]
      (.put buf ary offset length)
      (.position buf 0)
      buf)))

(defn array->direct-buffer
  (^ByteBuffer [ary]
     (array->buffer ary 0 (Array/getLength ^bytes ary)))
  (^ByteBuffer [ary ^long offset ^long length]
    (let [buf (ByteBuffer/allocateDirect length)]
      (.put buf ary offset length)
      (.position buf 0)
      buf)))

(def ^:private ^Class byte-array-class (Class/forName "[B"))

(defn input-stream->chunked-buffer
  [^InputStream input-stream ^long chunk-size]
  (let [close-fn #(.close input-stream)
        read-chunk (fn read-chunk []
                     (let [ary (Array/newInstance byte-array-class chunk-size)]
                       (loop [offset 0]
                         (let [len (.read input-stream ary offset (- chunk-size offset))
                               offset (+ offset len)]
                           (cond
                             (== -1 len)
                             (chunked-buffer (array->buffer ary 0 offset) nil close-fn)

                             (== chunk-size len)
                             (chunked-buffer (array->buffer ary 0 offset) read-chunk close-fn)

                             :else
                             (recur offset))))))]
    (read-chunk)))
