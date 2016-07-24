(ns talend-for-data-integration.chthree-xml-to-csv
  (:require [clojure.xml :as xml]
            [clojure.java.io :as io]
            [clojure.zip :as zip]
            [clojure.data.zip :as datazip]
            [clojure.data.zip.xml :as zip-xml]
            [clojure.pprint :as pp]
            [clojure.data.csv :as cd-csv]
            [semantic-csv.core :as sc]))

(def f
  (->
   "/home/ravi/myprogs/GETTINGSTARTEDTOS_DEMO_DATA/SampleDataFiles/Chapter3/catalogue.xml"
   io/input-stream
   xml/parse
   zip/xml-zip))

(def in-hash
  (for [sku (zip-xml/xml-> f :catalogue :sku)]
    (into {} (for [params (datazip/children sku)]
               [(:tag (first params))
                (zip-xml/text params)]))))

(with-open [out-file (io/writer "outfile.csv")]
  (->> in-hash
       sc/vectorize
       (cd-csv/write-csv out-file)))
