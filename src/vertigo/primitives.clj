(ns vertigo.primitives
  (:refer-clojure
    :exclude [+ - * / inc dec rem == not== zero? <= >= < >])
  (:use
    potemkin)
  (:require
    [primitive-math :as p])
  (:import
    [primitive_math
     Primitives]
    [java.nio
     ByteBuffer]))

;;;

(import-vars
  [primitive-math
   use-primitive-operators + - * / div inc dec rem == not== zero? <= >= < >])

(defn ->int8
  "Converts any number, character, or first character of a string to an int8."
  ^long [x]
  (long
    (cond
      (number? x) (byte x)
      (char? x) (-> x clojure.core/int byte)
      (string? x) (-> x first clojure.core/int byte)
      :else (throw (IllegalArgumentException. (str "Cannot convert " (pr-str x) " to byte."))))))

(import-fn p/byte    int8)
(import-fn p/short   int16)
(import-fn p/int     int32)
(import-fn p/long    int64)
(import-fn p/float   float32)
(import-fn p/double  float64)

(import-fn p/byte->ubyte int8->uint8)
(import-fn p/ubyte->byte uint8->int8)
(import-fn p/short->ushort int16->uint16)
(import-fn p/ushort->short uint16->int16)
(import-fn p/int->uint int32->uint32)
(import-fn p/uint->int uint32->int32)
(import-fn p/long->ulong int64->uint64)
(import-fn p/ulong->long uint64->int64)

(import-fn p/reverse-short reverse-int16)
(import-fn p/reverse-int reverse-int32)
(import-fn p/reverse-long reverse-int64)
(import-fn p/reverse-float reverse-float32)
(import-fn p/reverse-double reverse-float64)



