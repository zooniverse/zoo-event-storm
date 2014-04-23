(defproject zoo-storm "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Apache Public License 2.0"}
  :resource-paths ["resources"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :aot [zoo-storm.system]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [net.wurstmeister.storm/storm-kafka-0.8-plus "0.5.0-SNAPSHOT"]
                 [clj-http "0.7.8"]
                 [clj-time "0.6.0"]
                 [cheshire "5.3.1"]
                 [zmedelis/clj-geoip "0.1-SNAPSHOT"]
                 [paneer "0.2.0-SNAPSHOT"]
                 [korma "0.3.0-RC5"]
                 [org.clojure/math.combinatorics "0.0.7"]
                 [pg-json "0.2.0-SNAPSHOT"]
                 [org.clojure/core.match "0.2.1"]
                 [postgresql "9.1-901.jdbc4"]]
  :profiles {:dev 
             {:dependencies [[org.apache.storm/storm-core "0.9.2-incubating-SNAPSHOT"]]}
             :provided
             {:dependencies [[org.apache.storm/storm-core "0.9.2-incubating-SNAPSHOT"]]}})
