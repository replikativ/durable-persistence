(ns durable-persistence.b-tree-test
  (:require [durable-persistence.b-tree :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [clojure.core.async :refer [<!! chan]]
            [durable-persistence.refs :refer [load-ref create-ref]]
            [clojure.test :refer :all]))

(defn config [ib db ob]
  {:index-b ib :data-b db :op-buf-size ob})

(deftest simple-read-only-behavior
  (testing "Basic searches"
    (let [store (<!! (new-mem-store))
          data1 (<!! (create-ref store (data-node (config 3 3 2) (sorted-map 1 1 2 2 3 3 4 4 5 5))))
          data2 (<!! (create-ref store (data-node (config 3 3 2) (sorted-map 6 6 7 7 8 8 9 9 10 10))))
          root (->IndexNode [data1 data2] [] (config 3 3 2))]
      (is (= (lookup-key store root -10) nil) "not found key")
      (is (= (lookup-key store root 100) nil) "not found key")
      (dotimes [i 10]
        (is (= (lookup-key store root (inc i)) (inc i))))))
  (testing "basic fwd iterator"
    (let [store (<!! (new-mem-store))
          data1 (<!! (create-ref store (data-node (config 3 3 2) (sorted-map 1 1 2 2 3 3 4 4 5 5))))
          data2 (<!! (create-ref store (data-node (config 3 3 2) (sorted-map 6 6 7 7 8 8 9 9 10 10))))
          root (->IndexNode [data1 data2] [] (config 3 3 2))]
      (is (= (map first (lookup-fwd-iter store root 4)) (range 4 11)))
      (is (= (map first (lookup-fwd-iter store root 0)) (range 1 11)))))

  (testing "index nodes identified as such"
    (let [data (data-node (config 3 3 2) (sorted-map 1 1))
          root (->IndexNode [data] [] (config 3 3 2))]
      (is (index? root))
      (is (not (index? data))))))

