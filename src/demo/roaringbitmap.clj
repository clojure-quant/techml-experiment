(ns demo.roaringbitmap
  (:require
   [tick.core :as t]
   [tech.v3.dataset :as ds]
   [tablecloth.api :as tc]
   [tech.v3.libs.clj-transit :as tech-transit]
   [tech.v3.io :as io]))

(defn t->file
  [ds fname]
  (with-open [outs (io/output-stream! fname)]
    (tech-transit/dataset->transit ds outs)))

(defn sanitize-date [ds]
  (tc/convert-types ds {:date [:instant #(t/instant %)]}))

(-> (tc/dataset {:date [(t/zoned-date-time)]
                 :a [1]
                 :b [2.0]})
    (sanitize-date)
    (t->file "zoned.transit-json"))
;; now this does not throw error anymore. 
;; prior versions of techmlds had this error:
;; => Execution error at com.cognitect.transit.impl.AbstractEmitter/marshal (AbstractEmitter.java:195).
;;    Not supported: class org.roaringbitmap.RoaringBitmap


(-> (tc/dataset {:date [(t/zoned-date-time)]
                 :a [1]
                 :b [2.0]})
    (sanitize-date)
    (tech-transit/dataset->transit-str)
    (tech-transit/transit-str->dataset)
    )
;_unnamed [1 3]:
;
;|                    :date | :a |  :b |
;|--------------------------|---:|----:|
;| 2025-05-06T17:31:31.014Z |  1 | 2.0 |

