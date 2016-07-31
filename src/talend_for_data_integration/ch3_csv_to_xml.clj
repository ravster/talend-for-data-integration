(ns talend-for-data-integration.ch3-csv-to-xml
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as cd-csv]
            [clojure.data.xml :as cd-xml]
            [semantic-csv.core :as sc]))

(def f
  (with-open [in-file (io/reader "outfile.csv")]
    (->> in-file
         cd-csv/read-csv
         sc/mappify
         doall)))

(defn sku-child->xml [sku-child]
  (cd-xml/element (first sku-child)
                  {}
                  (second sku-child)))

(defn sku->xml [sku]
  (cd-xml/element :sku {}
                  (map sku-child->xml sku)))

(defn skus->xml [skus]
  (cd-xml/element :catalogue {}
                  (map sku->xml skus)))

(with-open [out-file (io/writer "out.xml")]
  (cd-xml/emit (skus->xml f)
               out-file))
