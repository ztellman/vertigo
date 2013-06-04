(ns vertigo.structs-test
  (:use
    clojure.test
    vertigo.test-utils)
  (:require
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]
    [vertigo.structs :as s]))

(def primitives
  [s/int8
   s/uint8
   s/int16
   s/int16-le
   s/int16-be
   s/uint16
   s/uint16-le
   s/uint16-be
   s/int32
   s/int32-le
   s/int32-be
   s/uint32
   s/uint32-le
   s/uint32-be
   s/int64
   s/int64-le
   s/int64-be
   s/uint64
   s/uint64-le
   s/uint64-be
   s/float32
   s/float32-le
   s/float64-be
   s/float64
   s/float64-le
   s/float64-be])

(deftest test-simple-roundtrips
  (doseq [typ primitives]
    (let [s (if (re-find #"float" (name typ))
              (map double (range 100))
              (range 100))]
      (is (= s (s/marshal-seq typ s)))
      (is (= s (s/lazily-marshal-seq typ s)))
      (is (= s (s/lazily-marshal-seq typ 32 s))))))

(s/def-typed-struct vec2
  :x s/float64
  :y s/int64)

(deftest test-compound-roundtrips
  (let [s (map
            #(hash-map :x %1 :y %2)
            (map double (range 100))
            (range 100))]
    (is (= s (s/marshal-seq vec2 s)))))

;;;

(deftest ^:benchmark benchmark-reduce
  (let [s (take 1e6 (range 1e6))]
    (bench "reduce unchunked seq"
      (reduce max s)))
  (let [s (range 1e6)]
    (bench "reduce chunked seq"
      (reduce max s)))
  (let [s (long-array 1e6)]
    (bench "reduce array"
      (reduce max s)))
  (let [s (s/marshal-seq s/int64 (range 1e6))]
    (bench "reduce int64 byte-seq"
      (reduce max s)))
  (let [s (s/marshal-seq s/int32 (range 1e6))]
    (bench "reduce int32 byte-seq"
      (reduce max s))))

(deftest ^:benchmark benchmark-marshalling
  (let [s (vec (range 10))]
    (bench "marshal 10 bytes"
      (s/marshal-seq s/int8 s)))
  (let [s (vec (range 100))]
    (bench "marshal 100 bytes"
      (s/marshal-seq s/int8 s)))
  (let [s (vec (range 1000))]
    (bench "marshal 1000 shorts"
      (s/marshal-seq s/int16 s)))
  (let [s (range 10)]
    (bench "lazily marshal 10 bytes"
      (s/marshal-seq s/int8 s)))
  (let [s (range 100)]
    (bench "lazily marshal 100 bytes"
      (s/lazily-marshal-seq s/int8 s)))
  (let [s (range 1000)]
    (bench "lazily marshal 1000 shorts"
      (s/lazily-marshal-seq s/int16 s))))
