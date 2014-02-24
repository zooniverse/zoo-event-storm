(defproject zoo-storm "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :resource-paths ["resources"]
  :aot [zoo-storm.json-scheme]
  :profiles {:dev 
             {:dependencies [[org.clojure/clojure "1.4.0"]
                             [storm "0.9.0.1"]
                             [net.wurstmeister.storm/storm-kafka-0.8-plus "0.2.0"]
                             [clj-http "0.7.8"]
                             [clj-time "0.6.0"]
                             [clj-kafka "0.1.2-0.8"]
                             [org.clojure/tools.namespace "0.2.3"]   
                             [cheshire "5.3.1"]
                             [zmedelis/clj-geoip "0.1"]
                             [paneer "0.2.0-SNAPSHOT"]
                             [korma "0.3.0-RC5"]
                             [org.clojure/core.match "0.2.1"]
                             [org.postgresql/postgresql "9.3-1101-jdbc41"]]}})
