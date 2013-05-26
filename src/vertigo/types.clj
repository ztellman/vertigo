(ns vertigo.types
  (:use
    potemkin)
  (:require
    [vertigo.bytes :as b]
    [vertigo.primitives :as p]))

;;;

(p/use-primitive-operators)

(let [s (fn [r w]
          [(partial list r) (partial list w)])
      u (fn [r w conv]
          [(fn [b idx]
             (list conv (list r b idx)))
           (fn [b idx val]
             (list w b idx (list conv val)))])]
  (def ^:private primitive-types
    (->>
      [:int8    1 (s `b/get-int8 `b/put-int8)
       :uint8   1 (u `b/get-int8 `b/put-int8 `p/byte->ubyte)
       :int16   2 (s `b/get-int16 `b/put-int16)
       :uint16  2 (u `b/get-int16 `b/put-int16 `p/short->ushort)
       :int32   4 (s `b/get-int32 `b/put-int32)
       :uint32  4 (u `b/get-int32 `b/put-int32 `p/int->uint)
       :int64   8 (s `b/get-int64 `b/put-int64)
       :uint64  8 (u `b/get-int64 `b/put-int64 `p/long->ulong)
       :float32 4 (s `b/get-float32 `b/get-float32)
       :float64 8 (u `b/get-float64 `b/put-float64)]
      (map
        (fn [type size [reader writer]]
          [type {:size size
                 :reader reader
                 :writer writer}]))
      (into {}))))

;;;

(defmacro def-primitive-type [& type+fields]
  (assert (even? (count type+fields)))
  (let [type+fields (partition 2 type+fields)]
    ))

(def)
