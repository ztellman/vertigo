(ns vertigo.bytes-test
  (:use
    clojure.test)
  (:require
    [vertigo.test-utils :as t]
    [criterium.core :as c]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]
    [clojure.java.io :as io])
  (:import
    [java.io
     ByteArrayInputStream]
    [java.nio
     ByteBuffer
     ByteOrder]
    [vertigo.bytes
     ByteSeq]))

(def types
  {:byte   [1 b/put-byte b/get-byte]
   :short  [2 b/put-short b/get-short]
   :int    [4 b/put-int b/get-int]
   :long   [8 b/put-long b/get-long]
   :float  [4 b/put-float b/get-float]
   :double [8 b/put-double b/get-double]})

(defn repeatedly-put [type s]
  (let [[byte-size put-f get-f] (types type)
        buf (->> byte-size (* (count s)) ByteBuffer/allocate b/byte-seq)]
    (doseq [[idx x] (map vector (iterate inc 0) s)]
      (put-f buf (* idx byte-size) x))
    buf))

(defn repeatedly-get [buf type cnt]
  (let [[byte-size put-f get-f] (types type)]
    (map
      #(get-f buf (* byte-size %))
      (range cnt))))

(defn byte-seq->input-stream [^ByteSeq byte-seq]
  (ByteArrayInputStream. (b/buffer->byte-array (.buf byte-seq))))

(defn ->chunked [byte-seq chunk-size]
  (-> byte-seq byte-seq->input-stream (b/input-stream->byte-seq chunk-size)))

(deftest test-roundtrip
  (doseq [t (keys types)]
    (let [s (if (#{:float :double} t)
              (map double (range 10))
              (range 10))
          [byte-size put-f get-f] (types t)]
      
      ;; basic roundtrip
      (is (= s (repeatedly-get (repeatedly-put t s) t 10)))
      
      ;; chunked roundtrip 
      (doseq [i (range 1 10)]
        (is (= s (repeatedly-get
                   (->chunked (repeatedly-put t s) (* byte-size i))
                   t 10))))
      
      ;; dropped roundtrip
      (is (= s (map
                 #(get-f
                    (b/drop-bytes
                      (repeatedly-put t s)
                      (* byte-size %))
                    0)
                 (range 10))))
      
      ;; dropped chunked roundtrip
      (doseq [i (range 1 10)]
        (is (= s (map
                   #(get-f
                      (b/drop-bytes
                        (->chunked (repeatedly-put t s) (* byte-size i))
                        (* byte-size %))
                      0)
                   (range 10))))))

    ))

;;;

(deftest ^:benchmark benchmark-raw-buffer
  (let [buf (ByteBuffer/allocateDirect 8)]
    (t/batch-bench "raw direct byte-buffer: get"
      (.getLong buf 0))
    (t/batch-bench "raw direct byte-buffer: put"
      (.putLong buf 0 0))
    (t/batch-bench "raw direct byte-buffer: get and put"
      (.putLong buf 0 (p/inc (.getLong buf 0))))))

(deftest ^:benchmark benchmark-byte-seq
  (let [bs (b/byte-seq (ByteBuffer/allocateDirect 8))]
    (t/batch-bench "byte-seq: get"
      (b/get-long bs 0))
    (t/batch-bench "byte-seq: put"
      (b/put-long bs 0 0))
    (t/batch-bench "byte-seq: get and put"
      (b/put-long bs 0 (p/inc (b/get-long bs 0))))))

(deftest ^:benchmark benchmark-chunked-byte-seq
  (let [bs (-> (ByteBuffer/allocateDirect 40) b/byte-seq (->chunked 8))]
    (t/batch-bench "chunked-byte-seq first chunk: get"
      (b/get-long bs 0))
    (t/batch-bench "chunked-byte-seq first chunk: put"
      (b/put-long bs 0 0))
    (t/batch-bench "chunked-byte-seq first chunk: get and put"
      (b/put-long bs 0 (p/inc (b/get-long bs 0))))

    (t/batch-bench "chunked-byte-seq fifth chunk: get"
      (b/get-long bs 32))
    (t/batch-bench "chunked-byte-seq fifth chunk: put"
      (b/put-long bs 32 0))
    (t/batch-bench "chunked-byte-seq fifth chunk: get and put"
      (b/put-long bs 32 (p/inc (b/get-long bs 32))))))
