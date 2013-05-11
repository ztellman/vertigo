(ns vertigo.core
  (:require
    [criterium.core])
  (:import
    [sun.misc
     Unsafe]
    [java.nio
     ByteOrder
     ByteBuffer]
    [vertigo.utils Primitives]))

(set! *unchecked-math* true)

(defrecord UnsafeBuffer
  [^Unsafe unsafe
   ^long location
   ^long bytes])

(defprotocol IMutable
  (get-val [_])
  (set-val [_ val]))

(deftype Unsynchronized
  [^:unsynchronized-mutable ^long val]
  IMutable
  (get-val [_] val)
  (set-val [_ v] (set! val (long v))))

(deftype Volatile
  [^:volatile-mutable ^long val]
  IMutable
  (get-val [_] val)
  (set-val [_ v] (set! val (long v))))

(def ^Unsafe unsafe
  (let [field (doto
                (.getDeclaredField Unsafe "theUnsafe")
                (.setAccessible true))]
    (.get field nil)))

(defn ^UnsafeBuffer allocate-buffer [^long bytes]
  (UnsafeBuffer. unsafe (.allocateMemory unsafe bytes) bytes))

(defmacro long-bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (criterium.core/bench
       (do ~@body)
       :reduce-with (fn [_# _#]))))

(defmacro bench [name & body]
  `(do
     (println "\n-----\n" ~name "\n-----\n")
     (criterium.core/quick-bench
       (do ~@body)
       :reduce-with #(and %1 %2))))

(defmacro repeat-body [n & body]
  `(do
     ~@(apply concat (repeat n body))))

(defn benchmark-read-writes []
  (let [buf (doto (ByteBuffer/allocateDirect 8)
              (.order (ByteOrder/nativeOrder)))]
    (bench "get/put direct"
      (dotimes [_ 1e3]
        (.putLong buf 0 (Primitives/add 1 (.getLong buf 0))))))
)


