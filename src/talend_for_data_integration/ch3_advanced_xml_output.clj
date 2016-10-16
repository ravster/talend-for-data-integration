(ns talend-for-data-integration.ch3-advanced-xml-output
  (:require [clojure.java.io :as io]
            [java-time :as time]
            [clojure.data.csv :as cd-csv]
            [clojure.data.xml :as cd-xml]
            [semantic-csv.core :as sc]))

(def file
  "/home/ravi/myprogs/GETTINGSTARTEDTOS_DEMO_DATA/SampleDataFiles/Chapter3/order_status.csv")

(def f
  "LazySeq of all rows in the CSV file."
  (with-open [in-file (io/reader file)]
    (-> in-file
        (cd-csv/read-csv :separator \;)
        sc/mappify
        doall)))

(def a
  "Filter out all non-shipped orders."
  (filter
   #(= "shipped" (:shipping_status %))
   f))

(def b
  (group-by :order_id a))

(defn transform [row]
  {:ID (:line_id row)
   :SKU (:sku row)
   :QUANTITY (:quantity row)
   :DISPATCHED_DATE (time/format
                     "y-M-d HH:mm"
                     (time/offset-date-time))
   :TRACKING_ID (:courier_docket_code row)})

(defn orderline->xml [row]
  (cd-xml/element "ORDER_LINE"
                  (transform row)))

(defn order->xml [order]
  (cd-xml/element "ORDER"
                  {:ID (first order)}
                  (map orderline->xml (second order))))

(defn orders->xml [orders]
  (cd-xml/element "DISPATCH_DOCKET"
                  {}
                  (map order->xml b)))

(with-open [out-file (io/writer "out.xml")]
  (cd-xml/emit (orders->xml b)
               out-file))
