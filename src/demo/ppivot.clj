(ns demo.ppivot
  (:require
   [tech.v3.dataset.rolling :as r]
   [tech.v3.datatype.functional :as dfn]
   [tech.v3.tensor :as dtt]
   [tech.v3.datatype :as dtype]
   [tablecloth.api :as tc]
   [tech.v3.dataset.column :as col]))

(defn clone-ds [d]
  (->> (tc/column-names d)
       (map (fn [col-n]
              [col-n (col/clone (get d col-n))]))
       (into {})
       (tc/dataset)))

(defn add-pivot
  "input is a tml dataset with bars (open high low close volume)
   adds pivot-low and pivot-high columns (both boolean)
   true at trailing extreme"
  [bar-ds {:keys [n min-bp]}]
  (let [; pivot detect
        bar-t-ds (r/rolling bar-ds
                            {:window-type :fixed
                             :window-size (inc (* 2 n)) ; 2n+1
                             :relative-window-position :center}
                            {:thigh (r/max :high)
                             :tlow (r/min :low)})
        pivot-low? (dtype/clone (dtype/emap dfn/eq :boolean (:low bar-ds) (:tlow bar-t-ds)))
        pivot-high? (dtype/clone (dtype/emap dfn/eq :boolean (:high bar-ds) (:thigh bar-t-ds)))
        ; dont allow pivot signal in the first n bars (trailing window has no data)
        ;_ (dtt/mset! (dtt/select pivot-low? (vec (range n))) [false])
        ;_ (dtt/mset! (dtt/select pivot-high? (vec (range n))) [false])
        ; dont allow pivot signal in the last n bars (trailing window has no data)
        rows (tc/row-count bar-ds)
        p (- rows n)
        ;_ (dtt/mset! (dtt/select pivot-low? (vec (range p rows))) [false])
        ;_ (dtt/mset! (dtt/select pivot-high? (vec (range p rows))) [false])
         ; min range check
        trailing-range (dtype/clone (dfn// (:thigh bar-t-ds) (:tlow bar-t-ds)))
        min-ratio (+ 1.0 (/ min-bp 10000.0))
        range? (dtype/clone (dfn/> trailing-range min-ratio))]
    (-> bar-ds
        (tc/add-columns
         {:pivot-low (dtype/clone (dfn/and pivot-low? range?))
          :pivot-high (dtype/clone (dfn/and pivot-high? range?))}))))


(defn recent-protective-pivot
  "returns a column that is true, if a protective pivot is found"
  [pivot? high-or-low close {:keys [min-ago
                                    max-ago
                                    max-distance-bp
                                    min-distance-bp
                                    high?]}]
  (let [recent-pivot-ago   (volatile! 0)
        recent-pivot-price (volatile! nil)
        n (count close)
        max-mult (+ 1.0 (/ max-distance-bp 10000.0))
        min-mult (+ 1.0 (/ min-distance-bp 10000.0))
        check-range (if high?
                      ; pivot is 10.0
                      (fn [current-price pivot-price]
                        ; example valid high-protective
                        ; price 9.8 -> lower 9.6 upper 10.6
                        ; (< 9.6 10.0 10.6)
                        (< (* current-price min-mult) pivot-price (* current-price max-mult)))
                      (fn [current-price pivot-price]
                        ; example valid low-protective
                        ; price 10.2 -> lower 9.6 upper 10.1
                        ; (< 9.6 10.0 10.1)
                        (< (/ current-price max-mult) pivot-price (/ current-price min-mult))))]
    ; dtype/clone is essential. otherwise on large datasets, the mapping will not
    ; be done in sequence, which means that the stateful mapping function will fail.
    (mapv (fn [idx]
            (if (pivot? idx)
              (do (vreset! recent-pivot-ago 0)
                  (vreset! recent-pivot-price (high-or-low idx))
                  false)
                                        ; no pivot
              (do (vswap! recent-pivot-ago inc)
                  (let [current-price (close idx)
                        pivot-price @recent-pivot-price]
                    (and pivot-price
                         (< min-ago @recent-pivot-ago max-ago)
                         (check-range current-price pivot-price)))))) (range n))))


(defn add-protective-pivot
  "input is a tml dataset with bars (open high low close volume)
   AND pivot calculated"
  [bar-ds {:keys [min-ago max-ago min-distance-bp max-distance-bp] :as opts}]
  (tc/add-columns
   bar-ds
   {:protective-pivot-low  (dtype/clone (recent-protective-pivot
                                         (:pivot-low bar-ds)
                                         (:low bar-ds)
                                         (:close bar-ds)
                                         (assoc opts :high? false)))
    :protective-pivot-high  (dtype/clone (recent-protective-pivot
                                          (:pivot-high bar-ds)
                                          (:high bar-ds)
                                          (:close bar-ds)
                                          (assoc opts :high? true)))}))

(defn add-pivot-screen-protective
  "input: tml dataset with bars (high low close)
   options pivot detection 
     n - pivot window (left/right)
     min-bp - minimum bp between min/max in pivot window
              to ignore pivots in sideway windows
   options pivot screening:
    relationship between close and most recent pivot
    min-ago max-ago - screen for pivots n bars ago
    min-distance-bp - screen 
   adds :pivot-low, :pivot-high, :protective-pivot-low :protective-pivot-high
   (all boolean columns)"
  [bar-ds {:keys [n min-bp
                  min-ago max-ago min-distance-bp max-distance-bp] :as opts}]
  (println "calculating add-pivot-protective " opts)
  (-> bar-ds
      (clone-ds) ; this helps sort out the tc/info issue
      (add-pivot {:n n :min-bp min-bp})
      (clone-ds); this helps sort out the tc/info issue
      (add-protective-pivot {:min-ago min-ago
                             :max-ago max-ago
                             :min-distance-bp min-distance-bp
                             :max-distance-bp max-distance-bp})
      (clone-ds) ; this helps sort out the tc/info issue
      ))

(comment


  (def ds
    (tc/dataset {:high   [1.0 1.2 1.3 1.4 1.5 1.4 1.3 1.2 1.1 1.2 1.25 1.27 1.3 1.31 1.30 1.31 1.30 1.31 1.30 1.31 1.30]
                 :low    [1.0 1.2 1.3 1.4 1.5 1.4 1.3 1.2 1.1 1.2 1.25 1.27 1.3 1.31 1.30 1.31 1.30 1.31 1.30 1.31 1.30]
                 :close  [1.0 1.2 1.3 1.4 1.5 1.4 1.3 1.2 1.1 1.2 1.25 1.27 1.3 1.31 1.30 1.31 1.30 1.31 1.30 1.31 1.30]}))

  (require '[juan.util :refer [print-ds]])

  (-> ds
      (add-pivot {:n 2 :min-bp 150.0})
      (add-protective-pivot {:min-ago 2
                             :max-ago 5
                             :min-distance-bp 1.0
                             :max-distance-bp 1000000.0})
      (print-ds))

; protective-pivot-high: 3x

  (-> (/ 1.50 1.20)
      (- 1.0)
      (* 10000.0)) ; 2500.0 bp 

  (-> (/ 1.50 1.1)
      (- 1.0)
      (* 10000.0)) ; 3636.3 bp

  (-> (/ 1.31 1.3)
      (- 1.0)
      (* 10000.0)) ; 333.33 bp

  ;; protective-pivot-low: 2x

  (-> (/ 1.27 1.10)
      (- 1.0)
      (* 10000.0)) ; 1545.45 bp

  (-> (/ 1.30 1.10)
      (- 1.0)
      (* 10000.0)) ; 1818.18 bp

  (-> ds
      (add-pivot {:n 2 :min-bp 150.0})
      (add-protective-pivot {:min-ago 2
                             :max-ago 5
                             :min-distance-bp 1600.0
                             :max-distance-bp 3000.0})
      (print-ds))

; protective-pivot-high: 1x
; filtered away:
; the 1. pivot is 40000bp, and max is 19501bp.
; the 3. pivot is 333bp, and min is 500bp

; protective-pivot-low: 1x
; the 1. pivot is 1545bp, and min is 1600bp.

;
  )


