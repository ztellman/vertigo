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

(import-fn p/byte   int8)
(import-fn p/short  int16)
(import-fn p/int    int32)
(import-fn long     int64)
(import-fn p/float  float32)
(import-fn double   float64)

(defn ->int8
  "Converts any number, character, or first character of a string to an int8."
  ^long [x]
  (long
    (cond
      (number? x) (byte x)
      (char? x) (-> x clojure.core/int byte)
      (string? x) (-> x first clojure.core/int byte)
      :else (throw (IllegalArgumentException. (str "Cannot convert " (pr-str x) " to byte."))))))

(defn int8->uint8
  "Converts an int8 to a uint8."
  {:inline (fn [x] `(->> ~x long (bit-and 0xFF) int16))}
  ^long [^long x]
  (long (int16 (bit-and x 0xFF))))

(defn uint8->int8
  "Converts a uint8 to an int8."
  {:inline (fn [x] `(int8 (long ~x)))}
  ^long [^long x]
  (long (int8 x)))

(defn int16->uint16
  "Converts an int16 to a uint16."
  {:inline (fn [x] `(->> ~x long (bit-and 0xFFFF) int32))}
  ^long [^long x]
  (long (int32 (bit-and 0xFFFF x))))

(defn uint16->int16
  "Converts a uint16 to an int16."
  {:inline (fn [x] `(int16 (long ~x)))}
  ^long [^long x]
  (long (int16 x)))

(defn int32->uint32
  "Converts an int32 to a uint32."
  {:inline (fn [x] `(->> ~x long (bit-and 0xFFFFFFFF)))}
  ^long [^long x]
  (long (int64 (bit-and 0xFFFFFFFF x))))

(defn uint32->int32
  "Converts a uint32 to an int32."
  {:inline (fn [x] `(int32 (long ~x)))}
  ^long [^long x]
  (long (int32 x)))

(defn int64->uint64
  "Converts an int64 to a uint64."
  [^long x]
  (BigInteger. 1
    (-> (ByteBuffer/allocate 8) (.putLong x) .array)))

(defn ^long uint64->int64
  "Converts a uint64 to an int64."
  ^long [x]
  (.longValue ^clojure.lang.BigInt (bigint x)))

(defn float32->int32
  "Converts a float32 to an int32."
  {:inline (fn [x] `(Float/floatToRawIntBits (p/float ~x)))}
  ^long [^double x]
  (long (Float/floatToRawIntBits x)))

(defn int32->float32
  "Converts an int32 to a float32."
  {:inline (fn [x] `(Float/intBitsToFloat (p/int ~x)))}
  ^double [^long x]
  (double (Float/intBitsToFloat x)))

(defn float64->int64
  "Converts a float32 to an int32."
  {:inline (fn [x] `(Double/doubleToRawLongBits ~x))}
  ^long [^double x]
  (long (Double/doubleToRawLongBits x)))

(defn int64->float64
  "Converts an int32 to a float32."
  {:inline (fn [x] `(Double/longBitsToDouble ~x))}
  ^double [^long x]
  (double (Double/longBitsToDouble x)))

;;;

(defn reverse-int16
  "Inverts the endianness of an int16."
  {:inline (fn [x] `(Primitives/reverseShort ~x))}
  ^long [^long x]
  (->> x Primitives/reverseShort long))

(defn reverse-int32
  "Inverts the endianness of an int32."
  {:inline (fn [x] `(Primitives/reverseInteger ~x))}
  ^long [^long x]
  (->> x Primitives/reverseInteger long))

(defn reverse-int64
  "Inverts the endianness of an int64."
  {:inline (fn [x] `(Primitives/reverseLong ~x))}
  ^long [^long x]
  (->> x Primitives/reverseLong long))

(defn reverse-float32
  "Inverts the endianness of a float32."
  {:inline (fn [x] `(-> ~x float32->int32 reverse-int32 int32->float32))}
  ^double [^double x]
  (-> x float32->int32 reverse-int32 int32->float32))

(defn reverse-float64
  "Inverts the endianness of a float32."
  {:inline (fn [x] `(-> ~x float32->int32 reverse-int32 int32->float32))}
  ^double [^double x]
  (-> x float64->int64 reverse-int64 int64->float64))


