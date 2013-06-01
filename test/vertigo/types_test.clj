(ns vertigo.types-test
  (:use
    clojure.test)
  (:require
    [vertigo.test-utils :as u]
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]
    [vertigo.types :as t]))

(deftest test-roundtrips
  (doseq [[k typ] @#'t/primitive-types]
    (let [s (if (#{:float32 :float64} k)
              (map double (range 100))
              (range 100))]
      (is (= s (->> s (t/encode-seq typ) (t/decode-byte-seq typ))))
      (is (= s (->> s (t/lazily-encode-seq typ) (t/decode-byte-seq typ))))
      (is (= s (->> s (t/lazily-encode-seq typ 32) (t/decode-byte-seq typ)))))))
