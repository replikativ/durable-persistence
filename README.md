# durable-persistence

*Note*: We have ported
the [hitchhiker-tree](https://github.com/datacrypt-project/hitchhiker-tree) as
it satisfies our durable persistence needs very well. The repository is here for
historical reasons.

This repository is for exploration of persistent datastructures on a durable
medium. The durable medium is abstracted away by a minimalistic
[key-value protocol](https://github.com/replikativ/konserve) providing atomic
operations (ACID) on single keys. This decouples the implementation of the
persistent datastructure from the host and backend store.

The repository contains an adaptation of the
[cookbook in-memory version](https://github.com/clojure-cookbook/clojure-cookbook/blob/master/02_composite-data/2-27_and_2-28_custom-data-structures/2-27_red-black-trees-part-i.asciidoc)
of a red black tree. Instead of keeping all fragments in memory we introduce a
Reference type to fragments in the store and dynamically load and store
fragments. The root can be assigned to a fixed key, modelling a durable identity
similar to a Clojure atom.

The long term motivation is to provide efficient indices as a building block for
[datatype management](https://github.com/replikativ/replikativ) independent of
JVM IO libraries spanning also to ClojureScript and the Browser. With konserve
an IndexedDB storage backend for the Browser already exists. For this reason IO
should happen asynchronously through `core.async` in `konserve`. A durable Index
for DataScript would be nice :).

## Usage

The easiest is to check out the project and hack it itself, but if you just want
to use it, you can also add this dependency:
[![Clojars Project](http://clojars.org/io.replikativ/durable-persistence/latest-version.svg)](http://clojars.org/io.replikativ/durable-persistence)



Here is a small benchmark and usage example:

~~~clojure
(require '[durable-persistence.redblack :refer :all]
         '[konserve.core :as k]
         '[konserve.filestore :refer [new-fs-store]]
         '[hasch.core :refer [uuid]]
         '[clojure.core.async :refer [go <!! <!]])


(def store (<!! (new-fs-store "/tmp/async-tree-experiment"
                              :read-handlers (atom {'async-trees.redblack.Ref map->Ref}))))

(def rb-tree (time (reduce (fn [rb-tree elem]
                             (<!! (go
                                    (let [st (System/currentTimeMillis)
                                          rb-tree (<! (insert-val store rb-tree elem))]
                                      (<! (k/assoc-in store [elem] [(uuid) [(uuid)]])) ;; add some data fragment
                                      (<! (k/assoc-in store [:root] rb-tree))
                                      (when (= (mod elem 1000) 0)
                                        (let [delta (- (System/currentTimeMillis)
                                                       st)]
                                          (println "Op for" elem " took " delta " ms")))
                                      rb-tree)))) nil (range 20000))))

;; printouts on the REPL
Op for 0  took  6  ms
Op for 1000  took  48  ms
Op for 2000  took  49  ms
Op for 3000  took  55  ms
Op for 4000  took  48  ms
Op for 5000  took  60  ms
Op for 6000  took  65  ms
Op for 7000  took  61  ms
Op for 8000  took  63  ms
Op for 9000  took  66  ms
Op for 10000  took  62  ms
Op for 11000  took  34  ms
Op for 12000  took  65  ms
Op for 13000  took  63  ms
Op for 14000  took  65  ms
Op for 15000  took  55  ms
Op for 16000  took  60  ms
Op for 17000  took  77  ms
Op for 18000  took  67  ms
Op for 19000  took  69  ms
"Elapsed time: 1239405.719974 msecs"
~~~

We can do efficient range queries on the index, too.

~~~clojure
(time (= (<!! (range-vals store rb-tree -1 20000))
              (range 20000)))
=> true
"Elapsed time: 7565.761136 msecs"
~~~

Shuffeling the inputs is better than worse case (in sequence):
~~~clojure
Op for 6000  took  33  ms
Op for 5000  took  22  ms
Op for 8000  took  35  ms
Op for 16000  took  39  ms
Op for 15000  took  33  ms
Op for 10000  took  31  ms
Op for 4000  took  38  ms
Op for 2000  took  37  ms
Op for 14000  took  47  ms
Op for 19000  took  53  ms
Op for 12000  took  29  ms
Op for 1000  took  45  ms
Op for 18000  took  51  ms
Op for 9000  took  25  ms
Op for 7000  took  48  ms
Op for 17000  took  56  ms
Op for 13000  took  44  ms
Op for 3000  took  50  ms
Op for 11000  took  56  ms
Op for 0  took  42  ms
"Elapsed time: 793967.016349 msecs"
~~~

## Garbage collection

20000 integer inserts cause almost 500 MiB garbage, because we repeatedly write
the index fragments and they have an edn serialization. This persistency
approach similar to Clojure's in memory one provides concurrency for operations
on multiple roots of shared fragments of the tree.

An approach along the lines of a mark and sweep collector together with a
monotonic timestamp on indices should provide short stop the world phases. This
needs to be done for a practical feasibility of course, but first the proper
datastructures should be found.

A copying garbage collector (to another store from the root) is easy to
implement already.

## Metadata format

This is reducible in size still, but focus at the moment is to find efficient
implementations without too much microoptimizations. Exploration of the design
space is still necessary.

## License

Copyright © 2016-2017 Christian Weilbach, 2016 David Greenberg

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

