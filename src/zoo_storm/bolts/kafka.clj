(ns zoo-storm.bolts.kafka
  (:require [clj-kafka.producer :refer [send-message producer message]]
            [clj-kafka.zk :refer [brokers]]
            [clojure.tools.logging :as log]
            [clojure.string :refer [join]]
            [backtype.storm.clojure :refer [emit-bolt! defbolt bolt ack!]]
            [cheshire.core :refer [generate-string]])
  (:gen-class))

(defbolt kafka-producer ["event"] {:prepare true}
  [config context collector]
  (let [bs (join "," (map #(str (:host %) ":" (:port %))
                          (brokers {"zookeeper.connect" "33.33.33.10:2181"})))
        p (producer {"metadata.broker.list" bs
                     "serializer.class" "kafka.serializer.DefaultEncoder"
                     "partitioner.class" "kafka.producer.DefaultPartitioner"})]
    (bolt
      (execute [tuple]
               (send-message 
                 p 
                 (message "events" 
                          (.getBytes (generate-string (tuple "event")))))
               (emit-bolt! collector tuple :anchor tuple)
               (ack! collector tuple)))))