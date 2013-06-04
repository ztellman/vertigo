(ns vertigo.primitives-test
  (:use
    clojure.test)
  (:require
    [criterium.core :as c]
    [vertigo.primitives :as p]))

(def primitive-ops
  {:int64    `[p/int64->uint64   p/uint64->int64   p/reverse-int64]
   :int32    `[p/int32->uint32   p/uint32->int32   p/reverse-int32]
   :int16    `[p/int16->uint16   p/uint16->int16   p/reverse-int16]
   :int8     `[p/int8->uint8     p/uint8->int8     identity]
   :float32  `[identity identity p/reverse-float32]
   :float64  `[identity identity p/reverse-float64]})

(deftest test-roundtrips
  (are [type nums]
    (let [[s->u u->s reverse-fn] (primitive-ops type)]
      (let [s->u' (eval s->u)
            u->s' (eval u->s)
            reverse-fn' (eval reverse-fn)]
        (every?
          (fn [x]
            (and
             ;; test both normal and inlined versions of the functions
              (= x (eval `(-> ~x ~s->u ~u->s)))
              (= x (-> x s->u' u->s'))
              (= x (eval `(-> ~x ~reverse-fn ~reverse-fn)))
              (= x (-> x reverse-fn' reverse-fn'))))
          nums)))
    
    :int64  [-1 0 1 Long/MIN_VALUE     Long/MAX_VALUE]
    :int32  [-1 0 1 Integer/MIN_VALUE  Integer/MAX_VALUE]
    :int16  [-1 0 1 Short/MIN_VALUE    Short/MAX_VALUE]
    :int8   [-1 0 1 Byte/MIN_VALUE     Byte/MAX_VALUE]))

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
