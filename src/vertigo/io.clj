(ns vertigo.io
  (:require
    [vertigo.structs :as s]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]
    [clojure.java.io :as io])
  (:import
    [java.io
     InputStream
     RandomAccessFile]
    [java.nio
     ByteBuffer
     MappedByteBuffer]
    [java.nio.channels
     FileChannel
     FileChannel$MapMode]))

(defn wrap-input-stream
  ([type input-stream]
     (wrap-input-stream type input-stream false 4096))
  ([type ^InputStream input-stream direct? chunk-size]
     (let [safe-chunk-size (p/* (s/byte-size type) (p/div (long chunk-size) (s/byte-size type)))]
       (s/wrap-byte-seq type
         (b/input-stream->byte-seq input-stream direct? safe-chunk-size)))))

(defn wrap-buffer
  ([type ^ByteBuffer buf]
     (->> buf b/byte-seq (s/wrap-byte-seq type))))

(defn wrap-array
  ([type ^bytes ary]
     (->> ary b/array->buffer b/byte-seq (s/wrap-byte-seq type))))

(defn wrap-file
  ([type filename]
     (->> filename io/reader io/input-stream (wrap-input-stream type))))

(defn wrap-mapped-file
  ([type ^String filename]
     (let [file (RandomAccessFile. filename "rw")
           ^MappedByteBuffer buf (-> file .getChannel (.map FileChannel$MapMode/READ_WRITE 0 (.length file)))]
       (s/wrap-byte-seq type
         (b/byte-seq buf
           (fn [_] (.close file))
           (fn [_] (.force buf)))))))

(defn marshal-seq
  "Converts a sequence into a marshaled version of itself."
  ([type s]
     (marshal-seq type true s))
  ([type direct? s]
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
     (lazily-marshal-seq type 4096 false s))
  ([type ^long chunk-byte-size direct? s]
     (let [stride (s/byte-size type)
           chunk-size (p/div chunk-byte-size stride)
           allocate (if direct? b/direct-buffer b/buffer)
           populate (fn populate [s]
                      (when-not (empty? s)
                        (let [remaining (promise)
                              nxt (delay (populate @remaining))
                              byte-seq (-> chunk-byte-size allocate (b/lazy-byte-seq nxt))]
                          (loop [idx 0, offset 0, s s]
                            (cond

                              (empty? s)
                              (do
                                (deliver remaining nil)
                                (b/slice byte-seq 0 offset))

                              (p/== chunk-size idx)
                              (do
                                (deliver remaining s)
                                byte-seq)

                              :else
                              (do
                                (s/write-value type byte-seq offset (first s))
                                (recur (p/inc idx) (p/+ offset stride) (rest s))))))))]
       (s/wrap-byte-seq type (populate s)))))
