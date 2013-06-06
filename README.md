Vertigo allows you to define mixed-type struct over byte-buffers which can be used like a normal Clojure data structure.  This allows for faster reads and reduced memory footprint, and can also make interop with C libraries significantly simpler.

### usage

```clj
[vertigo "0.1.0-SNAPSHOT"]
```

### defining a type

To define a typed structure, we use `vertigo.structs/def-typed-struct`:

```clj
(use 'vertigo.structs)

(def-typed-struct vec3
  :x float64
  :y int32
  :z uint8)
```

Each field is defined as a pair: a name and a type.  The resulting typed-struct can itself by used as a type:

```clj
(def-typed-struct two-vecs
  :a vec3
  :b vec3)
```

In the `vertigo.structs` namespace, there are a number of predefined primitive types: 

* `int8`, `int16`, `int32`, `int64`
** for unsigned variants, use `uint8`, etc.
* `float32`, `float64`
* add an `-le` or `-be` suffix to any primitive type for explicit endianness, otherwise it will default to the endianness of the underlying buffer

We can also define a fixed-length array of any type using `(array type length)`:

```clj
(def-typed-struct ints-and-floats
  :ints (array uint32 10)
  :floats (array float32 10))
```

### creating a sequence

To create a sequence, we can either _marshal_ an existing sequence onto a byte-buffer, or _wrap_ an existing byte source.  To marshal a sequence, we can either use `vertigo.structs/marshal-seq` or `vertigo.structs/lazily-marshal-seq`:

```clj
> (def s (range 5))
#'s
> (marshal-seq uint8 s)
(0 1 2 3 4)
```

While the marshaled seq is outwardly identical, under the covers it is just a thin object wrapping a 5 byte buffer.  In this case we could get an identical effect with an array, but this extends to types of any size or complexity:

```clj
> (def x {:ints (range 10), :floats (map float (range 10))})
#'x
> (marshal-seq ints-and-floats [x])
({:ints (0 1 2 3 4 5 6 7 8 9), :floats (0.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0)})
```

Using `marshal-seq` will realize and copy over the entire sequence at once.  For large or unbounded sequences, `lazily-marshal-seq` is preferred.  While the performance characteristics of the two methods may differ, the resulting seq is otherwise the same.

To create a sequence that wraps an already encoded source of bytes, you may use `vertigo.io/wrap-input-stream`, `vertigo.io/wrap-buffer`, et al.

### interacting with a sequence

While we can use `first`, `rest`, `nth`, `get-in` and all the other normal Clojure functions on these sequences, we can also do something other data structures can't.  Consider the above example of a map containing sequences of ints and floats.  To get the fifth element under the `:floats` key, we call `(get-in s [:floats 4])`.  This will first get the sequence under the `:floats` key, and then look for the fifth element within the sequence.  

However, in our data structure we're guaranteed to have a fixed layout, so _we know exactly where the data is already_.  We don't need to fetch all the intermediate data structures, we simply need to calculate the location and do a single read.  To do this, we use `vertigo.structs/get-in*`:

```clj
> (get-in* ^ints-and-floats s [:floats 4])
5
```

Notice that we have hinted the sequence with the type of the element that it contains.  Each call to `get-in*` must be hinted this way, otherwise it will be unable to calculate the offset at compile-time.  But in return for this slight inconvenience, we get a read time that even for moderately nested structures can be several orders of magnitude faster.

Since everything's just bytes under the covers, we can also overwrite existing values.  While this isn't always necessary, it's certainly useful when you need it.  To write to the sequence, we may either use `vertigo.structs/set-in!` or `vertigo.structs/update-in!`:

```clj
> (def s (marshal-seq int8 (range 5)))
#'s
> (set-in! ^int8 s [5] 42)
nil
> (update-in! ^int8 s [0] - 42)
nil
> s
(-42 1 2 3 42)
```

## license

Copyright © 2013 Zachary Tellman

Distributed under the [MIT License](http://opensource.org/licenses/MIT)
