(ns vertigo.io
  (:require
    [vertigo.structs :as s]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]
    [clojure.java.io])
  (:import
    [java.io
     InputStream
     RandomAccessFile]
    [java.nio
     MappedBytebuffer
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
  ([type filename]
     (let [file (RandomAccessFile. filename "rw")
           buf (-> file .getChannel (.map FileChannel$MapMode/READ_WRITE 0 (.length file)))]
       (s/wrap-byte-seq type
         (b/byte-seq buf #(do (.close file) (.close buf)))))))

