(ns vertigo.buffer-test
  (:use
    clojure.test)
  (:require
    [vertigo.buffer :as b]
    [clojure.java.io :as io])
  (:import
    [java.io
     ByteArrayInputStream]
    [java.nio
     ByteBuffer]
    [vertigo.buffer
     Buffer]))

(def types
  {:byte   [1 b/put-byte b/get-byte]
   :short  [2 b/put-short b/get-short]
   :int    [4 b/put-int b/get-int]
   :long   [8 b/put-long b/get-long]
   :float  [4 b/put-float b/get-float]
   :double [8 b/put-double b/get-double]})

(defn repeatedly-put [type s]
  (let [[byte-size put-f get-f] (types type)
        buf (->> byte-size (* (count s)) ByteBuffer/allocate b/buffer)]
    (doseq [[idx x] (map vector (iterate inc 0) s)]
      (put-f buf (* idx byte-size) x))
    buf))

(defn repeatedly-get [buf type cnt]
  (let [[byte-size put-f get-f] (types type)]
    (map
      #(get-f buf (* byte-size %))
      (range cnt))))

(defn buffer->input-stream [^Buffer buf]
  (ByteArrayInputStream. (.array ^ByteBuffer (.buf buf))))

(defn buffer->chunked-buffer [buf chunk-size]
  (-> buf buffer->input-stream (b/input-stream->chunked-buffer chunk-size)))

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
                   (buffer->chunked-buffer (repeatedly-put t s) (* byte-size i))
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
                        (buffer->chunked-buffer (repeatedly-put t s) (* byte-size i))
                        (* byte-size %))
                      0)
                   (range 10)))))

      )))
