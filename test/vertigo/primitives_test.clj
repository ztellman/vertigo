(ns vertigo.primitives-test
  (:use
    clojure.test)
  (:require
    [criterium.core :as c]
    [vertigo.primitives :as p]))

(def primitive-ops
  {:long  [p/long->ulong   p/ulong->long    p/reverse-long]
   :int   [p/int->uint     p/uint->int      p/reverse-int]
   :short [p/short->ushort p/ushort->short  p/reverse-short]
   :byte  [p/byte->ubyte   p/ubyte->byte    identity]})

(deftest test-roundtrips
  (are [type nums]
    (let [[s->u u->s reverse-fn] (primitive-ops type)]
      (every?
        #(and
           (= % (-> % s->u u->s))
           (= % (-> % reverse-fn reverse-fn)))
        nums))

    :long   [-1 0 1 Long/MIN_VALUE     Long/MAX_VALUE]
    :int    [-1 0 1 Integer/MIN_VALUE  Integer/MAX_VALUE]
    :short  [-1 0 1 Short/MIN_VALUE    Short/MAX_VALUE]
    :byte   [-1 0 1 Byte/MIN_VALUE     Byte/MAX_VALUE]))

(defn eval-assertions [x]
  (eval
    `(do
       ~@(map #(list `is %) x))))

(deftest test-arithmetic
  (p/use-primitive-operators)
  (try
    (eval-assertions
      '((== 6 (+ 1 2 3) (+ 3 3))
        (== 0 (- 6 3 3) (- 6 6))
        (== 12 (* 2 2 3) (* 4 3))
        (== 5 (/ 10 2) (/ 20 2 2) (/ 11 2))

        (== 6.0 (+ 1.0 2.0 3.0) (+ 3.0 3.0))

        (thrown? IllegalArgumentException
          (+ 1 2.0))))
    (finally
      (p/unuse-primitive-operators)))
  
  (eval-assertions
    `((== 6 (+ 1 2 3) (+ 3 3))
      (== 3.0 (+ 1 2.0))))

  )

;;;
