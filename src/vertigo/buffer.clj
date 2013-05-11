(ns vertigo.buffer
  (:use
    potemkin
    [potemkin.types :only (def-abstract-type)])
  (:require
    [vertigo.primitives :as prim])
  (:import
    [java.nio
     ByteBuffer]
    [vertigo.utils
     Primitives]
    [java.lang.reflect
     Array]))


(definterface+ IBuffer
  (get-byte [_ idx])
  (get-short [_ idx])
  (get-int [_ idx])
  (get-long [_ idx]))

(defmacro ^:private loop-and-get [this idx f]
  `(let [f# ~f]
     (loop [chunk# ~this, idx# ~idx]
       (let [next# (.next chunk#)
             next# (if (instance? clojure.lang.IFn next#)
                     (next#)
                     next#)]
         (if (or (not next#)
               (Primitives/lt idx# (.size chunk#)))
           (f# chunk# idx#)
           (recur next#
             (Primitives/subtract (long idx#) (.size chunk#))))))))

(declare chunked-buffer)

(deftype+ ABufferChunk
  [^ByteBuffer buf
   ^long size
   ^long chunk-size
   next]

  IBuffer
  (get-byte [this idx]
    (loop-and-get this idx
      (fn [^ByteBuffer buf ^long idx]
        (.get buf idx))))
  (get-short [this idx]
    (loop-and-get this idx
      (fn [^ByteBuffer buf ^long idx]
        (.getShort buf idx))))
  (get-int [this idx]
    (loop-and-get this idx
      (fn [^ByteBuffer buf ^long idx]
        (.getInt buf idx))))
  (get-long [this idx]
    (loop-and-get this idx
      (fn [^ByteBuffer buf ^long idx]
        (.getLong buf idx)))))

(defn direct-chunked-buffer
  [^bytes ary ^long offset ^long size next]
  (let [buf (ByteBuffer/allocateDirect chunk-size)
        len (Array/length ary)]
    
    (when ary
      (.put buf ary offset chunk-size)
      (.position buf 0))
    
    (DirectBufferChunk.
      buf
      len
      chunk-size
      next)))

(defn direct-chunked-buffer
  [^bytes ary ^long offset ^long size next]
  (let [buf (ByteBuffer/allocateDirect chunk-size)
        len (Array/length ary)]
    
    (when ary
      (.put buf ary offset size)
      (.position buf 0))
    
    (DirectBufferChunk.
      buf
      len
      size
      next)))
