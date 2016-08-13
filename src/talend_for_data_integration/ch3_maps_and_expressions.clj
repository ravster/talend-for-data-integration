(ns talend-for-data-integration.ch3-maps-and-expressions
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as cd-csv]
            [clojure.data.xml :as cd-xml]
            [semantic-csv.core :as sc]))

(def file
  "/home/ravi/myprogs/GETTINGSTARTEDTOS_DEMO_DATA/SampleDataFiles/Chapter3/expressions.csv")

(def f
  (with-open [in-file (io/reader file)]
    (-> in-file
        (cd-csv/read-csv :separator \;)
        sc/mappify
        doall)))

(defn transform-customer [customer]
  {:id (format "%08d" (Integer/parseInt (:CustomerID customer)))
   :name (clojure.string/join " " [(:FirstName customer)
                                   (:LastName customer)])
   :address_1 (clojure.string/join " " [(:Address1 customer)
                                        (:Address2 customer)])
   :address_2 (clojure.string/join " " [(:TownCity customer)
                                        (:County customer)
                                        (:Postcode customer)])
   :telephone_number (clojure.string/replace (:Telephone customer) #"[^0-9]" "")})

(def a (map transform-customer f))

(defn attr->xml [attr]
  (cd-xml/element (first attr)
                  {}
                  (second attr)))

(defn customer->xml [customer]
  (cd-xml/element "Customer"
                  {}
                  (map attr->xml customer)))

(defn customers->xml [customers]
  (cd-xml/element "Customers"
                  {}
                  (map customer->xml customers)))

(with-open [out-file (io/writer "out.xml")]
  (cd-xml/emit (customers->xml a)
               out-file))
