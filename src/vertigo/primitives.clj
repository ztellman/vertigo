(ns vertigo.primitives
  (:refer-clojure
    :exclude [* + - / < > == byte short int])
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
(variadic-proxy &&   vertigo.utils.Primitives/and)
(variadic-proxy ||   vertigo.utils.Primitives/or)
(variadic-proxy &&'  vertigo.utils.Primitives/bitAnd)
(variadic-proxy ||'  vertigo.utils.Primitives/bitOr)

(variadic-predicate-proxy >  vertigo.utils.Primitives/gt)
(variadic-predicate-proxy <  vertigo.utils.Primitives/lt)
(variadic-predicate-proxy == vertigo.utils.Primitives/eq)

;;;

(def ^:private vars-to-exclude '[* + - / < > == byte short int && || &&' ||'])

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
  ^long [^long x]
  (long (Primitives/toByte x)))

(defn short
  "Truncates a number to an int16."
  ^long [^long x]
  (long (Primitives/toShort x)))

(defn int
  "Truncates a number to an int32."
  ^long [^long x]
  (long (Primitives/toInteger x)))

;;;

(defn ->byte
  "Converts any number, character, or first character of a string to an int8."
  ^long [x]
  (long
    (cond
      (number? x) (byte x)
      (char? x) (-> x clojure.core/int byte)
      (string? x) (-> x first clojure.core/int byte)
      :else (throw (IllegalArgumentException. (str "Cannot convert " (pr-str x) " to byte."))))))

(defn byte->ubyte
  "Converts an int8 to a uint8."
  {:inline (fn [x] `(->> ~x long (Primitives/and 0xFF) Primitives/toShort))}
  ^long [^long x]
  (short (&&' x 0xFF)))

(defn ubyte->byte
  "Converts a uint8 to an int8."
  {:inline (fn [x] `(Primitives/toByte (long ~x)))}
  ^long [^long x]
  (byte x))

(defn short->ushort
  "Converts an int16 to a uint16."
  {:inline (fn [x] `(->> x long (Primitives/and 0xFF) Primitives/toInt))}
  ^long [^long x]
  (int (&&' 0xFFFF x)))

(defn ushort->short
  "Converts a uint16 to an int16."
  {:inline (fn [x] `(Primitives/toShort (long ~x)))}
  ^long [^long x]
  (short x))

(defn int->uint
  "Converts an int32 to a uint32."
  {:inline (fn [x] `(->> ~x long (Primitives/and 0xFFFFFFFF)))}
  ^long [^long x]
  (long (&&' 0xFFFFFFFF x)))

(defn uint->int
  "Converts a uint32 to an int32."
  {:inline (fn [x] `(Primitives/toInteger (long ~x)))}
  ^long [^long x]
  (int x))

(defn long->ulong
  "Converts an int64 to a uint64."
  [^long x]
  (BigInteger. 1
    (-> (ByteBuffer/allocate 8) (.putLong x) .array)))

(defn ^long ulong->long
  "Converts a uint64 to an int64."
  ^long [x]
  (.longValue ^clojure.lang.BigInt (bigint x)))

;;;

(defn reverse-short
  "Inverts the endianness of an int16."
  {:inline (fn [x] `(Primitives/reverseShort ~x))}
  ^long [^long x]
  (->> x Primitives/reverseShort long))

(defn reverse-int
  "Inverts the endianness of an int32."
  {:inline (fn [x] `(Primitives/reverseInteger ~x))}
  [^long x]
  (->> x Primitives/reverseInteger long))

(defn reverse-long
  "Inverts the endianness of an int64."
  {:inline (fn [x] `(Primitives/reverseLong ~x))}
  [^long x]
  (->> x Primitives/reverseLong long))
