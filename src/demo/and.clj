(ns demo.and
  (:require
   [tick.core :as t]
   [tech.v3.datatype :as dtype]
   [tech.v3.datatype.functional :as dfn]
   [tech.v3.tensor :as dtt]
   [tech.v3.dataset :as ds]
   [tablecloth.api :as tc]))


(defn and-multiple [& col-seq]
  (->> (reduce dfn/and (first col-seq) (rest col-seq))
       (into []) ; makes the result not lazy
       ))

(def a [1 2 3])
(def b [7 nil 9])
(def c [0 1 nil])

(dfn/and a b) ; correct: [true false true]
(dfn/and a c) ; correct: [true false true]
(dfn/and b c) ; correct: [true false false]
(dfn/and (dfn/and a b) c) ; correct: [true false false]
(and-multiple a b c); correct ; [true false false]
(apply and-multiple [a b c]) ; correct ; [true false false]


(defn add-and [ds name-seq]
  (let [cols (map (fn [col-name]
                    (get ds col-name)) name-seq)
        _ (println "cols: " cols)
        and-col (apply and-multiple cols)]
    (tc/add-column ds :and and-col)))

(def ds0 (tc/dataset {:a [true true true]
                      :b [true false true]
                      :c [true true false]}))

(add-and ds0 [:a :b :c])

; |   :a |    :b |    :c |  :and |
; |------|-------|-------|-------|
; | true |  true |  true |  true |
; | true | false |  true | false |
; | true |  true | false | false |


(def ds1 (tc/dataset {:a [1 2 3]
                      :b [7 nil 9]
                      :c [0 1 nil]}))

(add-and ds1 [:a :b :c])

; | :a | :b | :c |  :and |
; |---:|---:|---:|-------|
; |  1 |  7 |  0 |  true |
; |  2 |    |  1 |  true | ;; NOT CORRECT
; |  3 |  9 |    | false |


