(ns vertigo.structs-test
  (:use
    clojure.test
    vertigo.test-utils)
  (:require
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]
    [vertigo.structs :as s]))

(def prims s/primitive-types)

(deftest test-simple-roundtrips
  (doseq [[k typ] prims]
    (let [s (if (#{:float32 :float64} k)
              (map double (range 100))
              (range 100))]
      (is (= s (s/marshal-seq typ s)))
      (is (= s (s/lazily-marshal-seq typ s)))
      (is (= s (s/lazily-marshal-seq typ 32 s))))))

(s/def-primitive-struct vec2
  :x :float64
  :y :float64)

(deftest test-compound-roundtrips
  (let [s (map
            #(hash-map :x %1 :y %2)
            (map double (range 100))
            (map double (range 100)))]
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
  (let [s (s/marshal-seq (prims :int64) (range 1e6))]
    (bench "reduce int64 byte-seq"
      (reduce max s)))
  (let [s (s/marshal-seq (prims :int16) (range 1e6))]
    (bench "reduce int16 byte-seq"
      (reduce max s))))

(deftest ^:benchmark benchmark-marshalling
  (let [s (vec (range 10))
        int8 (prims :int8)]
    (bench "marshal 10 bytes"
      (s/marshal-seq int8 s)))
  (let [s (vec (range 100))
        int8 (prims :int8)]
    (bench "marshal 100 bytes"
      (s/marshal-seq int8 s)))
  (let [s (vec (range 1000))
        int16 (prims :int16)]
    (bench "marshal 1000 shorts"
      (s/marshal-seq int16 s)))
  (let [s (range 10)
        int8 (prims :int8)]
    (bench "lazily marshal 10 bytes"
      (s/marshal-seq int8 s)))
  (let [s (range 100)
        int8 (prims :int8)]
    (bench "lazily marshal 100 bytes"
      (s/lazily-marshal-seq int8 s)))
  (let [s (range 1000)
        int16 (prims :int16)]
    (bench "lazily marshal 1000 shorts"
      (s/lazily-marshal-seq int16 s))))
