(ns demo.vectorclone
  (:require
   [tech.v3.dataset.column :as col]
   [tech.v3.datatype.protocols]
   [tablecloth.api :as tc]))

;; JOINR WORKAROUND PRIOR TO DTYPE-NEXT-FIX 10.140

;; this fixes a bug of cloning columns that are vectors
;; requires clojure 1.12
;; see:
;; https://github.com/cnuernber/dtype-next/issues/129

;(extend-type clojure.lang.APersistentVector/1
;  tech.v3.datatype.protocols/PClone
;  (clone [this] (aclone (to-array (vec this)))))

;For clojure < 1.12:
;(extend-type (Class/forName "[Lclojure.lang.APersistentVector;")
;   tech.v3.datatype.protocols/PClone
;   (clone [this] (aclone (to-array (vec this)))))

(defn clone-ds [d]
  (->> (tc/column-names d)
       (map (fn [col-n]
              [col-n (col/clone (get d col-n))]))
       (into {})
       (tc/dataset)))


(def ds
  (tc/dataset  {:a [1 2 3]  :b [[2 3] [4] [5 5 5 5]]}))

ds

(clone-ds ds)
;; this throws if dtype-next prior to 10.140


(col/clone (:b ds))

(meta (:b ds))

(type (.ary-data (tech.v3.datatype/->array-buffer :persistent-vector [[2 3]])))
(class (.ary-data (tech.v3.datatype/->array-buffer :persistent-vector [[2 3]])))

(class [1 2])

