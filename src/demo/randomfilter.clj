(ns demo.randomfilter
  (:require
   [tick.core :as t]
   [tech.v3.datatype.functional :as dfn]
   [tech.v3.datatype :as dtype]
   [tech.v3.dataset.print :as p]
   [tech.v3.io :as io]
   [tablecloth.api :as tc]
   [demo.ppivot :refer [add-pivot-screen-protective]]))

(defn load-ds [filename]
  (let [s (io/gzip-input-stream filename)
        ds (io/get-nippy s)]
    ds))

(defn print-ds [ds]
  (-> ds
      (p/print-range :all)))


(def audusd
  (load-ds "audusd-ppivot.nippy"))


(-> audusd
    (add-pivot-screen-protective
     {:n 45 ; 45 min in both sides
      :min-bp 30.0
      :min-ago 60 ; 1 hour
      :max-ago (* 60 4) ; 4 hour
      :min-distance-bp 2.0 ; 
      :max-distance-bp 100.0})
    (tc/select-rows (fn [row]
                      (and
                       (t/> (:date row) (t/instant "2025-05-20T12:40:00Z"))
                       (t/< (:date row) (t/instant "2025-05-20T15:00:00Z"))
                       (or (:pivot-high row)
                           (:pivot-low row)
                           (:protective-pivot-low row)
                           (:protective-pivot-high row)
                           ))))
    (tc/select-columns [:date :pivot-low :protective-pivot-low :pivot-high :protective-pivot-high])
    print-ds
    ;(tc/info)
    )

    ; eval the last expression multiple times, it always returns different number of rows.

