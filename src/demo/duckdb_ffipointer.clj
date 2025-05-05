(ns demo.duckdb-ffipointer
  (:require
   [tablecloth.api :as tc]
   [tick.core :as t]
   [tech.v3.dataset :as ds]
   [tech.v3.io :as io]
   [tmducken.duckdb :as duckdb]
   [tech.v3.libs.clj-transit :refer [dataset->data data->dataset
                                     dataset->transit-str]]
   [tech.v3.dataset.column :as col]))

(def stocks
  (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                {:key-fn keyword
                 :dataset-name :stocks}))

(duckdb/initialize! {:duckdb-home "./binaries"})


(def db (duckdb/open-db "duckdb.ddb"))
(def conn (duckdb/connect db))

(duckdb/create-table! conn stocks)
(duckdb/insert-dataset! conn stocks)

(def ds2 (duckdb/sql->dataset conn "select * from stocks "))

ds2

(tc/select-rows ds2 (fn [row]
                      (and (t/> (get row "date") (t/date "2009-06-01"))
                          ; (= (get row "symbol") "AAPL")
                           )))



(defn save-ds [filename ds]
  (let [s (io/gzip-output-stream! filename)]
    (io/put-nippy! s ds)))

(save-ds "stocks.nippy.gz" ds2)
; Execution error (ExceptionInfo) at taoensso.nippy/throw-unfreezable (nippy.clj:953).
; Unfreezable type: class tech.v3.datatype.ffi.Pointer

(dataset->transit-str ds2)
; Execution error at com.cognitect.transit.impl.AbstractEmitter/marshal (AbstractEmitter.java:194).
; Not supported: class tech.v3.datatype.ffi.Pointer


(def col-symbol
  (ds2 "symbol"))

(def ds3
  (tc/dataset {:symbol (col/clone (ds2 "symbol"))
               :date (col/clone (ds2 "date"))
               :price (col/clone (ds2 "price"))}))

ds3

(save-ds "stocks3.nippy.gz" ds3)
(dataset->transit-str ds3)

(tc/column-names ds2)

(defn clone-ds [d]
  (->> (tc/column-names d)
       (map (fn [col-n]
              [col-n (col/clone (get d col-n))]))
       (into {})
       (tc/dataset)))

(->> (clone-ds ds2)
     (save-ds "stocks3.nippy.gz"))

(->> (clone-ds ds2)
     (dataset->transit-str))


(def d (dataset->data ds2))

(data->dataset d)

(dataset->data (data->dataset d))

