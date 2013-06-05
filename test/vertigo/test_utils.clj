(ns vertigo.test-utils
  (:require [criterium.core :as c]))

(defmacro long-bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (c/bench
       (do ~@body nil)
       :reduce-with #(and %1 %2))))

(defmacro bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (c/quick-bench
       (do ~@body nil)
       :reduce-with #(and %1 %2))))

(defmacro batch-bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (c/quick-bench
       (dotimes [_# 1e6] ~@body)
       :reduce-with #(and %1 %2))))
