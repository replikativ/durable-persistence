(ns durable-persistence.refs
  (:require [clojure.core.cache :as cache]
            [hasch.core :refer [uuid]]
            [konserve.core :as k]
            [clojure.core.async :refer [chan <! go <!!] :as async]))

(def C (atom (cache/lru-cache-factory {} :threshold 1024)))

(defrecord Ref [id])

(defn ref? [r]
  (= (type r) Ref))

(defn create-ref [store node]
  (go
    (when node
      (if (:id node) ;; TODO ??? (= (type node) async_tree_experiment.core.Ref)
        node
        (let [id (uuid node)]
          (when-not (or (get @C id)
                        (<! (k/exists? store id)))
            (swap! C assoc id node)
            (<! (k/assoc-in store [id] node)))
          (map->Ref {:id id}))))))

(defn load-ref [store ref]
  (go
    (let [{:keys [id]} ref]
      (or (get @C id)
          (let [v (<! (k/get-in store [id]))]
            (swap! C assoc id v)
            v)))))

