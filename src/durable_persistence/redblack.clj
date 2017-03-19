(ns durable-persistence.redblack
  (:require [clojure.core.async :refer [chan <! go <!!] :as async]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store]]
            [hasch.core :refer [uuid]]
            [clojure.core.match :refer [match]]
            [clojure.set :as set]
            [durable-persistence.refs :refer :all]))

;; thankfully adapted from https://github.com/clojure-cookbook/clojure-cookbook/blob/master/02_composite-data/2-27_and_2-28_custom-data-structures/2-27_red-black-trees-part-i.asciidoc


(defn load-tree-fragment [store tree depth]
  (go
   (if (= depth 0)
     tree
     (when tree
       (let [[color a y b]
             (if (:id tree)
               (let [v (or (get @C (:id tree))
                           (<! (k/get-in store [(:id tree)])))]
                 (when-not v
                   (throw (ex-info "Value should be in store:" tree)))
                 (swap! C assoc (:id tree) v)
                 v)
               tree)
             new-tree [color
                       (<! (load-tree-fragment store a (dec depth)))
                       y
                       (<! (load-tree-fragment store b (dec depth)))] ]
         new-tree)))))

(defn balance
  "Ensures the given subtree stays balanced by rearranging black nodes
  that have at least one red child and one red grandchild"
  [store tree depth]
  ;; fetch tree to grandchildren
  (go
   (let [tree (<! (load-tree-fragment store tree 2))]
     (match [tree]
            [(:or ;; Left child red with left red grandchild
              [:b [:r [:r a x b] y c] z d]
              ;; Left child red with right red grandchild
              [:b [:r a x [:r b y c]] z d]
              ;; Right child red with left red grandchild
              [:b a x [:r [:r b y c] z d]]
              ;; Right child red with right red grandchild
              [:b a x [:r b y [:r c z d]]])]
            ;; lets write some of the structure to disk here
            (if (= (mod depth 3) 0)
              (let [ar (<! (create-ref store a))
                    br (<! (create-ref store b))
                    cr (<! (create-ref store c))
                    dr (<! (create-ref store d))]
                #_(println "Writing at depth" depth " rebalance")
                [:r [:b ar x br] y [:b cr z dr]])
              [:r [:b a x b] y [:b c z d]])

            [[c1 [c2 a x b] y [c3 c z d]]]
            (if (= (mod depth 3) 0)
              (let [ar (<! (create-ref store a))
                    br (<! (create-ref store b))
                    cr (<! (create-ref store c))
                    dr (<! (create-ref store d))]
                #_(println "Writing at depth" depth)
                [c1 [c2 ar x br] y [c3 cr z dr]])
              [c1 [c2 a x b] y [c3 c z d]])

            :else tree))))

(defn insert-val
  "Inserts x in tree.
  Returns a node with x and no children if tree is nil.

  Returned tree is balanced. See also `balance`"
  [store tree x]
  (go
   (let [ins (fn ins [tree depth]
               (go
                (let [tree (<! (load-tree-fragment store tree 1))]
                  (match tree
                         nil [:r nil x nil]
                         [color a y b] (cond
                                         (< x y)
                                         (<! (balance store [color (<! (ins a (inc depth))) y b]
                                                      depth))
                                         (> x y)
                                         (<! (balance store [color a y (<! (ins b (inc depth)))]
                                                      depth))
                                         :else tree)))))
         [_ a y b] (<! (ins tree 1))]
     [:b a y b])))

(defn find-val
  "Finds value x in tree"
  [store tree x]
  (go
   (let [tree (<! (load-tree-fragment store tree 1))]
     (match tree
            nil nil
            [_ a y b]
            (cond
              (< x y) (<! (find-val store a x))
              (> x y) (<! (find-val store b x))
              :else x)))))

(defn range-vals
  [store tree s e]
  (go
    (let [tree (<! (load-tree-fragment store tree 1))]
      (match tree
             nil []
             [_ a y b]
             (cond
               (and (< s y) (> e y))
               (concat (<! (range-vals store a s e))
                       [y]
                       (<! (range-vals store b s e)))
               (< s y) (<! (range-vals store a s e))
               (> e y) (<! (range-vals store b s e))
               :else [])))))


(comment

  (def store (<!! #_(new-mem-store)
                  (new-fs-store "/tmp/async-tree-experiment-rb"
                                :read-handlers (atom {'durable_persistence.refs.Ref map->Ref}))))
  (count @assocs)

  (def perf-log (atom []))

  (def inserts (range 20000) #_(shuffle (range 20000)))

  (def rb-tree (time (reduce (fn [rb-tree elem]
                               (<!! (go
                                     (let [st (System/currentTimeMillis)
                                           rb-tree (<! (insert-val store rb-tree elem))]
                                       #_(<! (k/assoc-in store [elem] [(uuid) [(uuid)]]))
                                       (<! (k/assoc-in store [:root] rb-tree))
                                       #_(when-not (<??  (find-val store rb-tree elem))
                                         (throw (ex-info "Oooops." {:failed-to-insert elem
                                                                    :rb-tree rb-tree})))
                                       (when (= (mod elem 1000) 0)
                                         (let [delta (- (System/currentTimeMillis)
                                                        st)]
                                           (swap! perf-log conj delta)
                                           (println "Op for" elem " took " delta " ms")))
                                       rb-tree)))) nil inserts)))

  (time (= (<!! (range-vals store rb-tree -1 20000))
           (range 20000)))

  (count (pr-str (<!! (k/get-in store [:root]))))

  (count (pr-str rb-tree))
  ;; -> [:black [:black nil 0 nil] 1 [:black nil 2 [:red nil 3 nil]]]

  (<!! (find-val store rb-tree 2))
  ;; -> 2

  (time (<!! (find-val store rb-tree "hello")))
  ;; -> nil


  (=
   (uuid [:black nil 64 nil])

   (uuid [:black nil 72 nil])

   (uuid [:black nil 97 nil]))


  (def rb-tree (time (<?? (insert-val store rb-tree 5))))

  (time
   (doseq [i (range 10000)]
     (let [v (<?? (find-val store rb-tree i))]
       (if-not (= i v)
         (throw (ex-info "not equal" {:i i :stored v}))))))



  (defn read-rb-tree-values [rb-tree]
    (go-try
     (let [tree (<? (load-tree rb-tree))
           [_ a y b] tree]
       (match tree
              [_ nil nil nil] #{}
              [_ nil y nil] #{y}
              [_ nil y b] (set/union #{y} (<? (read-rb-tree-values b)))
              [_ a y nil] (set/union #{y} (<? (read-rb-tree-values a)))
              [_ a y b] (set/union (<? (read-rb-tree-values a)) #{y} (<? (read-rb-tree-values b)))))))

  (def values (<?? (read-rb-tree-values rb-tree)))

  (count values)

  (<?? (count-rb-tree rb-tree))




  )


