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

;:_unnamed [560 3]:
;| symbol |       date |  price |
;|--------|------------|-------:|
;|   MSFT | 2000-01-01 |  39.81 |
;|   MSFT | 2000-02-01 |  36.35 |
;|   MSFT | 2000-03-01 |  43.22 |
;|   MSFT | 2000-04-01 |  28.37 |
;|   MSFT | 2000-05-01 |  25.45 |
;|   MSFT | 2000-06-01 |  32.54 |
;|   MSFT | 2000-07-01 |  28.40 |
;|   MSFT | 2000-08-01 |  28.40 |
;|   MSFT | 2000-09-01 |  24.53 |
;|   MSFT | 2000-10-01 |  28.02 |
;|    ... |        ... |    ... |
;|   AAPL | 2009-05-01 | 135.81 |
;|   AAPL | 2009-06-01 | 142.43 |
;|   AAPL | 2009-07-01 | 163.39 |
;|   AAPL | 2009-08-01 | 168.21 |
;|   AAPL | 2009-09-01 | 185.35 |
;|   AAPL | 2009-10-01 | 188.50 |
;|   AAPL | 2009-11-01 | 199.91 |
;|   AAPL | 2009-12-01 | 210.73 |
;|   AAPL | 2010-01-01 | 192.06 |
;|   AAPL | 2010-02-01 | 204.62 |
;|   AAPL | 2010-03-01 | 223.02 |



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

;; NOTE: 1. load from duckdb 2. use tablecloth/select/rows 3. clone columns
;;      => result dataset has the selected column correct (here :date), but 
;;         price column has wrong values.

(-> ds2
    (tc/select-rows (fn [row]
                      (or (= (get row "price") 39.81)
                          (= (get row "price") 36.35)
                          (= (get row "price") 43.22)
                          (= (get row "price") 28.37)))))

;:_unnamed [5 3]:
;| symbol |       date | price |
;|--------|------------|------:|
;|   MSFT | 2000-01-01 | 39.81 | -> this goes to AAPL 2009-07-01
;|   MSFT | 2000-02-01 | 36.35 | -> this goes to AAPL 2009-08-01
;|   MSFT | 2000-03-01 | 43.22 | -> this goes to AAPL 2009-09-01
;|   MSFT | 2000-04-01 | 28.37 | -> this goes to AAPL 2009-10-01
;|   AMZN | 2005-01-01 | 43.22 |

; price column values seem to come from indices from the ORIGINAL dataset, 
; NOT from the selection.


(defn dummy-select2 [ds]
  (tc/select-rows ds (fn [row]
                       (and (t/> (get row "date") (t/date "2009-06-01"))
                            (or (= (get row "symbol") "AAPL")
                                (= (get row "symbol") "AMZN"))))))

(-> ds2
    dummy-select2
    cloneds)

;_unnamed [18 3]:
;| symbol |       date |  price |
;|--------|------------|-------:|
;|   AMZN | 2009-07-01 |  85.76 |
;|   AMZN | 2009-08-01 |  81.19 |
;|   AMZN | 2009-09-01 |  93.36 |
;|   AMZN | 2009-10-01 | 118.81 |
;|   AMZN | 2009-11-01 | 135.91 |
;|   AMZN | 2009-12-01 | 134.52 |
;|   AMZN | 2010-01-01 | 125.41 |
;|   AMZN | 2010-02-01 | 118.40 |
;|   AMZN | 2010-03-01 | 128.82 |
;|   AAPL | 2009-07-01 | 163.39 |
;|   AAPL | 2009-08-01 | 168.21 |
;|   AAPL | 2009-09-01 | 185.35 |
;|   AAPL | 2009-10-01 | 188.50 |
;|   AAPL | 2009-11-01 | 199.91 |
;|   AAPL | 2009-12-01 | 210.73 |
;|   AAPL | 2010-01-01 | 192.06 |
;|   AAPL | 2010-02-01 | 204.62 |
;|   AAPL | 2010-03-01 | 223.02 |

;; THIS ONE WORKS OK - HOW IS THIS POSSIBLE ???


(defn dummy-select3 [ds]
  (tc/select-rows ds (fn [row]
                       (and (t/> (get row "date") (t/date "2009-06-01"))
                            (= (get row "symbol") "AMZN")))))

(-> ds2
    dummy-select3
    cloneds)

;_unnamed [9 3]:
;| symbol |       date | price |
;|--------|------------|------:|
;|   AMZN | 2009-07-01 | 39.81 |
;|   AMZN | 2009-08-01 | 36.35 |
;|   AMZN | 2009-09-01 | 43.22 |
;|   AMZN | 2009-10-01 | 28.37 |
;|   AMZN | 2009-11-01 | 25.45 |
;|   AMZN | 2009-12-01 | 32.54 |
;|   AMZN | 2010-01-01 | 28.40 |
;|   AMZN | 2010-02-01 | 28.40 |
;|   AMZN | 2010-03-01 | 24.53 |

; IDENTICAL PRICES AS IF SYMBOL AAPL

;; TEST FOR DIRECT FROM CSV

(defn dummy-select3 [ds]
  (tc/select-rows ds (fn [row]
                       (and (t/> (get row :date) (t/date "2009-06-01"))
                            (= (get row :symbol) "AMZN")))))

(-> stocks
    dummy-select3
    cloneds)

;_unnamed [9 3]:
;| :symbol |      :date | :price |
;|---------|------------|-------:|
;|    AMZN | 2000-01-01 |  39.81 |
;|    AMZN | 2000-02-01 |  36.35 |
;|    AMZN | 2000-03-01 |  43.22 |
;|    AMZN | 2000-04-01 |  28.37 |
;|    AMZN | 2000-05-01 |  25.45 |
;|    AMZN | 2000-06-01 |  32.54 |
;|    AMZN | 2000-07-01 |  28.40 |
;|    AMZN | 2000-08-01 |  28.40 |
;|    AMZN | 2000-09-01 |  24.53 |