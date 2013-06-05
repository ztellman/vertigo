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
  {:int8    [1 b/put-int8 b/get-int8]
   :int16   [2 b/put-int16 b/get-int16]
   :int32   [4 b/put-int32 b/get-int32]
   :int64   [8 b/put-int64 b/get-int64]
   :float32 [4 b/put-float32 b/get-float32]
   :float64 [8 b/put-float64 b/get-float64]})

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
    (let [s (if (#{:float32 :float64} t)
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
  (let [buf (.order (ByteBuffer/allocateDirect 8) (ByteOrder/nativeOrder))]
    (t/batch-bench "raw direct byte-buffer: get"
      (.getLong buf 0))
    (t/batch-bench "raw direct byte-buffer: put"
      (.putLong buf 0 0))
    (t/batch-bench "raw direct byte-buffer: get and put"
      (.putLong buf 0 (p/inc (.getLong buf 0)))))
  (let [buf (.order (ByteBuffer/allocate 8) (ByteOrder/nativeOrder))]
    (t/batch-bench "raw indirect byte-buffer: get"
      (.getLong buf 0))
    (t/batch-bench "raw indirect byte-buffer: put"
      (.putLong buf 0 0))
    (t/batch-bench "raw indirect byte-buffer: get and put"
      (.putLong buf 0 (p/inc (.getLong buf 0))))))

(deftest ^:benchmark benchmark-array
  (let [ary (long-array 1)]
    (t/batch-bench "array: get"
      (aget ary 0))
    (t/batch-bench "arrat: put"
      (aset ary 0 0))
    (t/batch-bench "array: get and put"
      (aset ary 0 (p/inc (long (aget ary 0)))))))

(deftest ^:benchmark benchmark-byte-seq
  (let [bs (b/byte-seq (ByteBuffer/allocateDirect 8))]
    (t/batch-bench "byte-seq: get"
      (b/get-int64 bs 0))
    (t/batch-bench "byte-seq: put"
      (b/put-int64 bs 0 0))
    (t/batch-bench "byte-seq: get and put"
      (b/put-int64 bs 0 (p/inc (b/get-int64 bs 0)))))
  (let [bs (b/byte-seq (ByteBuffer/allocate 8))]
    (t/batch-bench "indirect byte-seq: get"
      (b/get-int64 bs 0))
    (t/batch-bench "indirect byte-seq: put"
      (b/put-int64 bs 0 0))
    (t/batch-bench "indirect byte-seq: get and put"
      (b/put-int64 bs 0 (p/inc (b/get-int64 bs 0))))))

(deftest ^:benchmark benchmark-chunked-byte-seq
  (let [bs (-> (ByteBuffer/allocateDirect 40) b/byte-seq (->chunked 8))]
    (t/batch-bench "chunked-byte-seq first chunk: get"
      (b/get-int64 bs 0))
    (t/batch-bench "chunked-byte-seq first chunk: put"
      (b/put-int64 bs 0 0))
    (t/batch-bench "chunked-byte-seq first chunk: get and put"
      (b/put-int64 bs 0 (p/inc (b/get-int64 bs 0))))

    (t/batch-bench "chunked-byte-seq fifth chunk: get"
      (b/get-int64 bs 32))
    (t/batch-bench "chunked-byte-seq fifth chunk: put"
      (b/put-int64 bs 32 0))
    (t/batch-bench "chunked-byte-seq fifth chunk: get and put"
      (b/put-int64 bs 32 (p/inc (b/get-int64 bs 32))))))
