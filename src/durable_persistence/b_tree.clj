(ns durable-persistence.b-tree
  (:require [clojure.core.rrb-vector :refer [catvec subvec]]
            [clojure.core.async :refer [chan <! go <!!] :as async]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [konserve.filestore :refer [new-fs-store]]
            [hasch.core :refer [uuid]]
            [clojure.set :as set]
            [durable-persistence.refs :refer :all])
  (:import [java.util Arrays Collections]))





(defprotocol IKeyCompare
  (compare [key1 key2]))

(defprotocol IResolve
  "All nodes must implement this protocol. It's includes the minimal functionality
   necessary to avoid resolving nodes unless strictly necessary."
  (index? [_] "Returns true if this is an index node")
  (last-key [_ store] "Returns the rightmost key of the node"))

(defn tree-node?
  [node]
  (satisfies? IResolve node))

(defprotocol INode
  (overflow? [node] "Returns true if this node has too many elements")
  (underflow? [node] "Returns true if this node has too few elements")
  (merge-node [node other] "Combines this node with the other to form a bigger node. We assume they're siblings")
  (split-node [node store] "Returns a Split object with the 2 nodes that we turned this into")
  (lookup [node store key] "Returns the child node which contains the given key"))

(defrecord Split [left right median])

(extend-protocol IKeyCompare
  ;; By default, we use the default comparator
  Object
  (compare [key1 key2] (clojure.core/compare key1 key2))
  Double
  (compare [^Double key1 key2]
    (if (instance? Double key2)
      (.compareTo key1 key2)
      (clojure.core/compare key1 key2)))
  Long
  (compare [^Long key1 key2]
    (if (instance? Long key2)
      (.compareTo key1 key2))
    (clojure.core/compare key1 key2)))

;; TODO enforce that there always (= (count children) (inc (count keys)))
;; why?
;;
(declare data-node)


(defn index-node-keys
  "Calculates the separating keys given the children of an index node"
  [store children]
  (mapv (fn [c]
          (last-key (<!! (load-ref store c)) store))
        (butlast children)))

(defrecord IndexNode [children op-buf cfg]
  IResolve
  (index? [this] true)
  (last-key [this store]
    ;;TODO should optimize by caching to reduce IOps (can use monad)
    (last-key (<!! (load-ref store (peek children))) store))
  INode
  (overflow? [this]
    (>= (count children) (* 2 (:index-b cfg))))
  (underflow? [this]
    (< (count children) (:index-b cfg)))
  (split-node [this store]
    (let [b (:index-b cfg)
          median (nth (index-node-keys store children) (dec b))
          [left-buf right-buf] (split-with #(not (pos? (compare (:key %) median)))
                                           ;;TODO this should use msg/affects-key
                                           (sort-by :key op-buf))]
      (->Split (->IndexNode (subvec children 0 b)
                            (vec left-buf)
                            cfg)
               (->IndexNode (subvec children b)
                            (vec right-buf)
                            cfg)
              median)))
  (merge-node [this other]
    (->IndexNode (catvec children (:children other))
                 (catvec op-buf (:op-buf other))
                 cfg))
  (lookup [root store key]
    ;;This is written like so because it's performance critical
    (let [l (dec (count children))
          a (object-array l)
          _ (dotimes [i l]
              (let [c (nth children i)]
                ;; TODO remove ref check
                (aset a i (last-key (if (ref? c) (<!! (load-ref store c)) c) store))))
          x (Arrays/binarySearch a 0 l key compare)]
      (if (neg? x)
        (- (inc x))
        x))))



(defn index-node?
  [node]
  (instance? IndexNode node))

(defn nth-of-set
  "Like nth, but for sorted sets. O(n)"
  [set index]
  (first (drop index set)))

(defrecord DataNode [children cfg]
  IResolve
  (index? [this] false)
  (last-key [this store]
    (when (seq children)
      (-> children
          (rseq)
          (first)
          (key))))
  INode
  ;; Should have between b & 2b-1 children
  (overflow? [this]
    (>= (count children) (* 2 (:data-b cfg))))
  (underflow? [this]
    (< (count children) (:data-b cfg)))
  (split-node [this store]
    (->Split (data-node cfg (into (sorted-map-by compare) (take (:data-b cfg)) children))
             (data-node cfg (into (sorted-map-by compare) (drop (:data-b cfg)) children))
             (nth-of-set children (dec (:data-b cfg)))))
  (merge-node [this other]
    (data-node cfg (into children (:children other))))
  (lookup [root store key]
    (let [x (Collections/binarySearch (vec (keys children)) key compare)]
      (if (neg? x)
        (- (inc x))
        x))))

(defn data-node
  "Creates a new data node"
  [cfg children]
  (->DataNode children cfg))

(defn data-node?
  [node]
  (instance? DataNode node))

(defn backtrack-up-path-until
  "Given a path (starting with root and ending with an index), searches backwards,
   passing each pair of parent & index we just came from to the predicate function.
   When that function returns true, we return the path ending in the index for which
   it was true, or else we return the empty path"
  [path pred]
  (loop [path path]
    (when (seq path)
      (let [from-index (peek path)
            tmp (pop path)
            parent (peek tmp)]
        (if (pred parent from-index)
          path
          (recur (pop tmp)))))))

(defn right-successor
  "Given a node on a path, find's that node's right successor node"
  [store path]
  ;(clojure.pprint/pprint path)
  ;TODO this function would benefit from a prefetching hint
  ;     to keep the next several sibs in mem
  (when-let [common-parent-path
             (backtrack-up-path-until
               path
               (fn [parent index]
                 (< (inc index) (count (:children parent)))))]
    (let [next-index (-> common-parent-path peek inc)
          parent (-> common-parent-path pop peek)
          new-sibling (<!! (load-ref store (nth (:children parent) next-index)))
          ;; We must get back down to the data node
          sibling-lineage (into []
                                (take-while #(or (index-node? %)
                                                 (data-node? %)))
                                (iterate #(let [c (<!! (load-ref store (-> % :children first)))]
                                            c)
                                         new-sibling))
          path-suffix (-> (interleave sibling-lineage
                                      (repeat 0))
                          (butlast)) ; butlast ensures we end w/ node
          ]
      (-> (pop common-parent-path)
          (conj next-index)
          (into path-suffix)))))

(defn forward-iterator
  "Takes the result of a search and returns an iterator going
   forward over the tree. Does lg(n) backtracking sometimes."
  [store path start-key]
  (let [start-node (peek path)]
    (if-not (data-node? start-node) (prn "SN" start-node))
    (assert (data-node? start-node))
    (let [first-elements (-> start-node
                             :children ; Get the indices of it
                             (subseq >= start-key)) ; skip to the start-index
          next-elements (lazy-seq
                          (when-let [succ (right-successor store (pop path))]
                            (forward-iterator store succ start-key)))]
      (concat first-elements next-elements))))

(defn lookup-path
  "Given a B-tree and a key, gets a path into the tree"
  [store tree key]
  (loop [path [tree] ;alternating node/index/node/index/node... of the search taken
         cur tree ;current search node
         ]
    (if (seq (:children cur))
      (if (data-node? cur)
        path
        (let [index (lookup cur store key)
              child (if (data-node? cur)
                      nil #_(nth-of-set (:children cur) index)
                      (-> (:children cur)
                          ;;TODO what are the semantics for exceeding on the right? currently it's trunc to the last element
                          (nth index (peek (:children cur)))))
              child (<!! (load-ref store child))
              path' (conj path index child)]
          (recur path' child)))
      nil)))

(defn lookup-key
  "Given a B-tree and a key, gets an iterator into the tree"
  ([store tree key]
   (lookup-key store tree key nil))
  ([store tree key not-found]
   (-> (lookup-path store tree key)
       (peek)
       :children
       (get key not-found))))

(defn lookup-fwd-iter
  [store tree key]
  (let [path (lookup-path store tree key)]
    (prn "p" path)
    (when path
      (forward-iterator store path key))))

(comment
  (let [test-tree (<!! (load-ref store (<!! (k/get-in store [:root]))))]
    (lookup-fwd-iter store test-tree 450)))

(defn insert
  [store tree-ref key value]
  (let [{:keys [cfg] :as tree} (<!! (load-ref store tree-ref))
        path (lookup-path store tree key)
        {:keys [children] :or {children (sorted-map-by compare)}} (peek path)
        updated-data-node (data-node cfg (assoc children key value))]
    (loop [node updated-data-node
           path (pop path)]
      (if (empty? path)
        (if (overflow? node)
          (let [{:keys [left right median]} (split-node node store)
                node (->IndexNode [(<!! (create-ref store left))
                                   (<!! (create-ref store right))] [] cfg)]
            (<!! (create-ref store node)))
          (<!! (create-ref store node)))
        (let [index (peek path)
              {:keys [children keys] :as parent} (peek (pop path))]
          (if (overflow? node) ; splice the split into the parent
            ;;TODO refactor paths to be node/index pairs or 2 vectors or something
            (let [{:keys [left right median]} (split-node node store)
                  new-children (catvec (conj (subvec children 0 index)
                                             (<!! (create-ref store left))
                                             (<!! (create-ref store right)))
                                       (subvec children (inc index)))]
              (recur (-> parent
                         (assoc :children new-children))
                     (pop (pop path))))
            (recur (-> parent
                       ;;TODO this assoc-in seems to be a bottleneck
                       (assoc-in [:children index] (<!! (create-ref store node))))
                   (pop (pop path)))))))))

;;TODO: cool optimization: when merging children, push as many operations as you can
;;into them to opportunisitcally minimize overall IO costs


;; TODO incognito not deserializing nested ref records ???

(def store (<!! #_(new-mem-store)
                (new-fs-store "/tmp/async-tree-experiment"
                              #_:write-handlers #_(atom {durable_persistence.b_tree.IndexNode (fn [n] #_(prn "in" n) n)
                                                     durable_persistence.b_tree.DataNode (fn [n] #_(prn "dn" n) n)})
                              :read-handlers (atom {'durable_persistence.refs.Ref map->Ref
                                                    'durable_persistence.b_tree.DataNode (fn [{:keys [children cfg]}]
                                                                                           (->DataNode (into (sorted-map-by compare) children) cfg))
                                                    'durable_persistence.b_tree.IndexNode (fn [{:keys [children cfg]}]
                                                                                            (->IndexNode (->> children
                                                                                                              (map second)
                                                                                                              (map map->Ref)
                                                                                                              vec
                                                                                                              catvec) (catvec) cfg))})
                              :config {:fsync true})))




(defn b-tree
  [store cfg & kvs]
  (let [dnode (data-node cfg (sorted-map-by compare))
        init (reduce (fn [t [k v]]
                       (insert store t k v))
                     dnode
                     (partition 2 kvs))]
    (<!! (create-ref store init))))


;; TODO load from storage with catvec

(comment
  (time
   (let [config {:index-b 500 :data-b 500 :op-buf-size 5}
         root (b-tree store config) 
         full (reduce (fn [t i] (insert store t i i)) root (range 10))
         #_empty #_(reduce (fn [t i] (delete t i)) full [10 11 12 13 14 15 16 17 18 19])
         ]
     
     (lookup-key store (<!! (load-ref store full)) 5)
     
     #_(depth full)
     #_(clojure.pprint/pprint empty)
     #_(delete full 3)
     #_(dotimes [i 10]
         (is (= (lookup-key root (inc i)) (inc i))))))

  (def perf-log (atom []))

  (def inserts (range 50000) #_(shuffle (range 50)))

  (def test-tree (time (reduce (fn [test-tree elem]
                                 (<!! (go
                                        (let [st (System/currentTimeMillis)
                                              test-tree (insert store test-tree elem elem)]
                                          #_(<! (k/assoc-in store [elem] [(uuid) [(uuid)]]))
                                          (<! (k/assoc-in store [:root] test-tree))
                                          #_(when-not (<??  (find-val store test-tree elem))
                                              (throw (ex-info "Oooops." {:failed-to-insert elem
                                                                         :test-tree test-tree})))
                                          (when (= (mod elem 100) 0)
                                            (let [delta (- (System/currentTimeMillis)
                                                           st)]
                                              (swap! perf-log conj delta)
                                              (println "Op for" elem " took " delta " ms")))
                                          test-tree))))
                               (b-tree store {:index-b 500 :data-b 500 :op-buf-size 5})
                               inserts)))

  (let [test-tree (<!! (load-ref store (<!! (k/get-in store [:root]))))
        s (take 100 (shuffle (range 5000)))]
    (for [i s]
      (time (= (lookup-key store test-tree i) i))))

  (let [test-tree (<!! (load-ref store (<!! (k/get-in store [:root]))))]
    (lookup-fwd-iter store test-tree 450))

  (let [test-tree (<!! (load-ref store (<!! (k/get-in store [:root]))))]
    (time (lookup-key store test-tree 4028))))


(defn delete
  [store tree-ref key]
  (let [{:keys [cfg] :as tree} (<!! (load-ref store tree-ref))
        path (lookup-path store tree key) ; don't care about the found key or its index
        {:keys [children] :or {children (sorted-map-by compare)}} (peek path)
        updated-data-node (data-node cfg (dissoc children key))]
    (loop [node updated-data-node
           path (pop path)]
      (if (empty? path)
        ;; Check for special root underflow case
        (if (and (index-node? node) (= 1 (count (:children node))))
          (first (:children node))
          (<!! (create-ref store node)))
        (let [index (peek path)
              {:keys [children keys op-buf] :as parent} (peek (pop path))]
          (if (underflow? node) ; splice the split into the parent
            ;;TODO this needs to use a polymorphic sibling-count to work on serialized nodes
            (let [bigger-sibling-idx
                  (cond
                    (= (dec (count children)) index) (dec index) ; only have left sib
                    (zero? index) 1 ;only have right sib
                    (> (count (:children (nth children (dec index))))
                       (count (:children (nth children (inc index)))))
                    (dec index) ; right sib bigger
                    :else (inc index))
                  node-first? (> bigger-sibling-idx index) ; if true, `node` is left
                  merged (if node-first?
                           (merge-node node (nth children bigger-sibling-idx))
                           (merge-node (nth children bigger-sibling-idx) node))
                  old-left-children (subvec children 0 (min index bigger-sibling-idx))
                  old-right-children (subvec children (inc (max index bigger-sibling-idx)))]
              (if (overflow? merged)
                (let [{:keys [left right median]} (split-node merged store)]
                  (recur (<!! (create-ref store
                                          (->IndexNode (catvec (conj old-left-children left right)
                                                               old-right-children)
                                                       op-buf
                                                       cfg)))
                         (pop (pop path))))
                (recur (<!! (create-ref store
                                        (->IndexNode (catvec (conj old-left-children merged)
                                                             old-right-children)
                                                     op-buf
                                                     cfg)))
                       (pop (pop path)))))
            (recur (<!! (create-ref (->IndexNode (assoc children index node)
                                                 op-buf
                                                 cfg)))
                   (pop (pop path)))))))))

(comment

  (let [test-ref (<!! (k/get-in store [:root]))]
    [(lookup-key store (<!! (load-ref store test-ref)) 5)
     (lookup-key store (delete store test-ref 5) 5)])

  (time (reduce (fn [test-tree elem]
                  (<!! (go
                         (let [st (System/currentTimeMillis)
                               test-tree (delete store test-tree elem)]
                           #_(<! (k/assoc-in store [elem] [(uuid) [(uuid)]]))
                           (<! (k/assoc-in store [:root] test-tree))
                           #_(when-not (<??  (find-val store test-tree elem))
                               (throw (ex-info "Oooops." {:failed-to-insert elem
                                                          :test-tree test-tree})))
                           (when (= (mod elem 100) 0)
                             (let [delta (- (System/currentTimeMillis)
                                            st)]
                               (swap! perf-log conj delta)
                               (println "Op for" elem " took " delta " ms")))
                           test-tree))))
                test-tree
                #_(b-tree store {:index-b 1000 :data-b 1000 :op-buf-size 5})
                (drop-last inserts)))


  (<!! (load-ref store #durable_persistence.refs.Ref{:id #uuid "1d875d29-6447-501c-919a-eb34ac5dac42"}))



  (def bar (<!! (k/log store :foo)))

  (doseq [i (range 10000)]
    (let [st (System/currentTimeMillis)]
      (<!! (k/append store :foo i))
      (when (zero? (mod i 100))
        (let [delta (- (System/currentTimeMillis)
                       st)]
          (println "Op for" i " took " delta " ms")))))

  (<!! (k/reduce-log store :foo (fn [old e] (conj old e)) []))

  )

