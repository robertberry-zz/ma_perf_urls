(defproject ma_perf_urls "1.0.0-SNAPSHOT"
  :description "Mobile Aggregator performance test urls"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-time "0.5.1"]
                 [clj-ssh "0.5.6"]
                 [org.clojure/tools.logging "0.2.6"]]
  :plugins [[lein-ring "0.4.5"]]
  :main ma_perf_urls.core)
