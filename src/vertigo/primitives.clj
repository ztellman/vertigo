(ns vertigo.primitives
  (:refer-clojure
    :exclude [* + - / < > <= >= == byte short int float inc dec zero?])
  (:use
    potemkin)
  (:require
    [robert.hooke :as hooke])
  (:import
    [java.nio
     ByteBuffer]
    [vertigo.utils
     Primitives]))

;;;

(defmacro ^:private variadic-proxy [name fn]
  `(defmacro ~name
     ([x# y#]
        (list '~fn x# y#))
     ([x# y# ~'& rest#]
        (list* '~name (list '~name x# y#) rest#))))

(defmacro ^:private variadic-predicate-proxy [name fn]
  `(defmacro ~name
     ([x# y#]
        (list '~fn x# y#))
     ([x# y# ~'& rest#]
        (list 'vertigo.utils.Primitives/and (list '~name x# y#) (list* '~name y# rest#)))))

(variadic-proxy +    vertigo.utils.Primitives/add)
(variadic-proxy -    vertigo.utils.Primitives/subtract)
(variadic-proxy *    vertigo.utils.Primitives/multiply)
(variadic-proxy /    vertigo.utils.Primitives/divide)
(variadic-proxy div  vertigo.utils.Primitives/divide)
(variadic-proxy &&   vertigo.utils.Primitives/and)
(variadic-proxy ||   vertigo.utils.Primitives/or)
(variadic-proxy &&'  vertigo.utils.Primitives/bitAnd)
(variadic-proxy ||'  vertigo.utils.Primitives/bitOr)

(variadic-predicate-proxy >   vertigo.utils.Primitives/gt)
(variadic-predicate-proxy <   vertigo.utils.Primitives/lt)
(variadic-predicate-proxy <=  vertigo.utils.Primitives/lte)
(variadic-predicate-proxy >=  vertigo.utils.Primitives/gte)
(variadic-predicate-proxy ==  vertigo.utils.Primitives/eq)

(defmacro inc [x]
  `(Primitives/inc ~x))

(defmacro dec [x]
  `(Primitives/dec ~x))

(defmacro zero? [x]
  `(Primitives/isZero ~x))

;;;

(def ^:private vars-to-exclude
  '[* + - / < > <= >= == byte short int float && || &&' ||' inc dec zero?])

(defn- using-primitive-operators? []
  (= #'vertigo.primitives/+ (resolve '+)))

(defn use-primitive-operators
  "Replaces Clojure's arithmetic and number coercion functions with primitive equivalents.  These are
   defined as macros, so they cannot be used as higher-order functions.  This is an idempotent operation.."
  []
  (when-not (using-primitive-operators?)
    (doseq [v vars-to-exclude]
      (ns-unmap *ns* v))
    (require (vector 'vertigo.primitives :refer vars-to-exclude))))

(defn unuse-primitive-operators
  "Undoes the work of `use-primitive-operators`.  This is idempotent."
  []
  (doseq [v vars-to-exclude]
    (ns-unmap *ns* v))
  (refer 'clojure.core))

(defn- ns-wrapper
  "Makes sure that if a namespace that is using primitive operators is reloaded, it will automatically
   exclude the shadowed operators in `clojure.core`."
  [f & x]
  (if-not (using-primitive-operators?)
    (apply f x)
    (let [refer-clojure (->> x
                          (filter #(and (sequential? %) (= :refer-clojure (first %))))
                          first)
          refer-clojure-clauses (update-in
                                  (apply hash-map (rest refer-clojure))
                                  [:exclude]
                                  #(concat % vars-to-exclude))]
      (apply f
        (concat
          (remove #{refer-clojure} x)
          [(list* :refer-clojure (apply concat refer-clojure-clauses))])))))

(hooke/add-hook #'clojure.core/ns #'ns-wrapper)

;;;

(defn byte 
  "Truncates a number to an int8."
  {:inline (fn [x] `(vertigo.utils.Primitives/toByte ~x))}
  ^long [^long x]
  (long (Primitives/toByte x)))

(defn short
  "Truncates a number to an int16."
  {:inline (fn [x] `(vertigo.utils.Primitives/toShort ~x))}
  ^long [^long x]
  (long (Primitives/toShort x)))

(defn int
  "Truncates a number to an int32."
  {:inline (fn [x] `(vertigo.utils.Primitives/toInteger ~x))}
  ^long [^long x]
  (long (Primitives/toInteger x)))

(defn float
  "Truncates a number to a float32."
    {:inline (fn [x] `(vertigo.utils.Primitives/toFloat ~x))}
  ^double [^double x]
  (double (Primitives/toFloat x)))

(import-fn byte   int8)
(import-fn short  int16)
(import-fn int    int32)
(import-fn long   int64)
(import-fn float  float32)
(import-fn double float64)

;;;

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
  {:inline (fn [x] `(->> ~x long (Primitives/bitAnd 0xFF) Primitives/toShort))}
  ^long [^long x]
  (long (int16 (&&' x 0xFF))))

(defn uint8->int8
  "Converts a uint8 to an int8."
  {:inline (fn [x] `(Primitives/toByte (long ~x)))}
  ^long [^long x]
  (long (int8 x)))

(defn int16->uint16
  "Converts an int16 to a uint16."
  {:inline (fn [x] `(->> ~x long (Primitives/bitAnd 0xFF) Primitives/toInteger))}
  ^long [^long x]
  (long (int32 (&&' 0xFFFF x))))

(defn uint16->int16
  "Converts a uint16 to an int16."
  {:inline (fn [x] `(Primitives/toShort (long ~x)))}
  ^long [^long x]
  (long (int16 x)))

(defn int32->uint32
  "Converts an int32 to a uint32."
  {:inline (fn [x] `(->> ~x long (Primitives/bitAnd 0xFFFFFFFF)))}
  ^long [^long x]
  (long (int64 (&&' 0xFFFFFFFF x))))

(defn uint32->int32
  "Converts a uint32 to an int32."
  {:inline (fn [x] `(Primitives/toInteger (long ~x)))}
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
  {:inline (fn [x] `(Float/floatToRawIntBits (Primitives/toFloat ~x)))}
  ^long [^double x]
  (long (Float/floatToRawIntBits x)))

(defn int32->float32
  "Converts an int32 to a float32."
  {:inline (fn [x] `(Float/intBitsToFloat (Primitives/toInteger ~x)))}
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
