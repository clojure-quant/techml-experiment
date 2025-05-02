(ns demo.clone
  (:require
   [tablecloth.api :as tc]
   [tick.core :as t]
   [tech.v3.dataset :as ds]
   [tech.v3.dataset.column :as col]
   [tech.v3.io :as io]
   [tmducken.duckdb :as duckdb]))


(def stocks
  (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                {:key-fn keyword
                 :dataset-name :stocks}))

(duckdb/initialize! {:duckdb-home "./binaries"})


(def db (duckdb/open-db "clone.ddb"))
(def conn (duckdb/connect db))

(duckdb/create-table! conn stocks)
(duckdb/insert-dataset! conn stocks)

(def ds2 (duckdb/sql->dataset conn "select * from stocks "))

ds2


(defn dummy-select [ds]
  (tc/select-rows ds (fn [row]
                       (and (t/> (get row "date") (t/date "2009-06-01"))
                            (= (get row "symbol") "AAPL")))))


(-> ds2
    dummy-select)

;:_unnamed [9 3]:
;| symbol |       date |  price |
;|--------|------------|-------:|
;|   AAPL | 2009-07-01 | 163.39 |
;|   AAPL | 2009-08-01 | 168.21 |
;|   AAPL | 2009-09-01 | 185.35 |
;|   AAPL | 2009-10-01 | 188.50 |
;|   AAPL | 2009-11-01 | 199.91 |
;|   AAPL | 2009-12-01 | 210.73 |
;|   AAPL | 2010-01-01 | 192.06 |
;|   AAPL | 2010-02-01 | 204.62 |
;|   AAPL | 2010-03-01 | 223.02 |


(defn cloneds [d]
  (->> (tc/column-names d)
       (map (fn [col-n]
              [col-n (col/clone (get d col-n))]))
       (into {})
       (tc/dataset)))


(-> ds2
    dummy-select
    cloneds)

;_unnamed [9 3]:
;| symbol |       date | price |
;|--------|------------|------:|
;|   AAPL | 2009-07-01 | 39.81 |
;|   AAPL | 2009-08-01 | 36.35 |
;|   AAPL | 2009-09-01 | 43.22 |
;|   AAPL | 2009-10-01 | 28.37 |
;|   AAPL | 2009-11-01 | 25.45 |
;|   AAPL | 2009-12-01 | 32.54 |
;|   AAPL | 2010-01-01 | 28.40 |
;|   AAPL | 2010-02-01 | 28.40 |
;|   AAPL | 2010-03-01 | 24.53 |






