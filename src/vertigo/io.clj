(ns vertigo.io
  (:require
    [vertigo.structs :as s]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]
    [byte-streams :as convert])
  (:import
    [java.io
     File
     RandomAccessFile
     Closeable]
    [java.nio
     ByteBuffer
     MappedByteBuffer]
    [java.nio.channels
     Channels
     FileChannel
     FileChannel$MapMode]))

(defn- safe-chunk-size [type ^long chunk-size]
  (p/* (s/byte-size type) (p/div chunk-size (s/byte-size type))))

(defn wrap
  ([type x]
     (wrap type x nil))
  ([type x
    {:keys [direct? chunk-size writable?]
     :or {direct? false
          chunk-size (if (string? x)
                       (int 2e9)
                       (int 1e6))
          writable? true}
     :as options}]
     (let [x' (if (string? x) (File. ^String x) x)
           chunk-size (safe-chunk-size type chunk-size)]
       (let [bufs (convert/to-byte-buffers x' options)]
         (s/wrap-byte-seq type
           (if (empty? (rest bufs))
             (b/byte-seq (first bufs))
             (b/to-chunked-byte-seq bufs))
           (when (instance? Closeable bufs)
             (fn [] (.close ^Closeable bufs)))
           (when (or (instance? File x) (string? x))
             (fn [] (map #(.force ^MappedByteBuffer %) bufs)))))))) 

(defn marshal-seq
  "Converts a sequence into a marshaled version of itself."
  ([type s]
     (marshal-seq type s nil))
  ([type s {:keys [direct?] :or {direct? true}}]
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
     (lazily-marshal-seq type s nil))
  ([type s {:keys [chunk-size direct?] :or {chunk-size 65536}}]
     (let [chunk-size (long (safe-chunk-size type chunk-size))
           allocate (if direct? b/direct-buffer b/buffer)
           stride (s/byte-size type)
           populate (fn populate [s]
                      (when-not (empty? s)
                        (let [remaining (promise)
                              nxt (delay (populate @remaining))
                              byte-seq (-> chunk-size allocate (b/chunked-byte-seq nxt))]
                          (loop [idx 0, offset 0, s s]
                            (cond

                              (empty? s)
                              (do
                                (deliver remaining nil)
                                (b/slice byte-seq 0 offset))

                              (p/== chunk-size offset)
                              (do
                                (deliver remaining s)
                                byte-seq)

                              :else
                              (do
                                (s/write-value type byte-seq offset (first s))
                                (recur (p/inc idx) (p/+ offset stride) (rest s))))))))]
       (s/wrap-byte-seq type (populate s)))))
