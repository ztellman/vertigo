(ns vertigo.core-test
  (:use
    clojure.test
    vertigo.test-utils)
  (:require
    [vertigo.core :as c]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]
    [vertigo.structs :as s]
    [vertigo.io :as io]))

(def int-matrix (s/array s/int64 10 10))

(def ^:int-matrix int64-matrices
  (io/marshal-seq int-matrix
    (->> (range 1e3)
      (partition 10)
      (partition 10))))

(def array-dim 1e6)

(def ^:s/int64 int64-array
  (io/marshal-seq s/int64 (range array-dim)))

(def ^:s/int32 int32-array
  (io/marshal-seq s/int32 (range array-dim)))

(def ^:s/int16 int16-array
  (io/marshal-seq s/int16 (range array-dim)))

(deftest test-over
  (are [expected fields]
    (= expected (c/over int64-matrices fields))
    
    (range 0 10)     [0 0 _]
    (range 0 1000)   [_ _ _]
    (range 990 1000) [9 9 _]
    (range 600 700)  [6 _ _]

    ;; 90-100, 190-200, ...
    (mapcat #(range (+ (* % 100) 90) (* (inc %) 100)) (range 10))
    [_ 9 _]

    ;; 101, 111, 121, ...
    (map #(+ 100 (* % 10) 1) (range 10))
    [1 _ 1]))

(deftest test-doreduce
  (is (= (reduce + (range 1e3))
        (c/doreduce [x int64-array] [sum 0]
          (+ x sum)))))

;;;

(deftest ^:benchmark benchmark-reduce-matrix-sum
  (let [^:s/int64 s (c/over int64-matrices [0 0 _])]
    (bench "pre-over 1d sum"
      (c/doreduce [x s] [sum 0]
        (p/+ sum x))))
  (bench "simple 1d sum"
    (c/doreduce [x (over int64-array [_])] [sum 0]
      (p/+ sum x)))
  (bench "1d sum"
    (c/doreduce [x (over int64-matrices [0 0 _])] [sum 0]
      (p/+ sum x)))
  (bench "2d sum"
    (c/doreduce [x (over int64-matrices [0 _ _])] [sum 0]
      (p/+ sum x)))
  (bench "3d sum"
    (c/doreduce [x (over int64-matrices [_ _ _])] [sum 0]
      (p/+ sum x))))

(deftest ^:benchmark benchmark-reduce-sum
  (let [num-array (long-array (range array-dim))]
    (bench "array sum"
      (reduce + num-array)))
  (let [s (range array-dim)]
    (bench "seq sum"
      (reduce + s)))
  (let [s (take array-dim (iterate inc 0))]
    (bench "lazy-seq sum"
      (reduce + s)))
  (bench "doreduce sum int64"
    (c/doreduce [x int64-array] [sum 0]
      (p/+ sum x)))
  (bench "doreduce sum int32"
    (c/doreduce [x int32-array] [sum 0]
      (p/+ sum x)))
  (bench "doreduce sum int16"
    (c/doreduce [x int16-array] [sum 0]
      (p/+ sum x))))
