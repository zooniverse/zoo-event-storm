(ns zoo-storm.bolts.kafka
  (:require [clj-kafka.producer :refer [send-message producer message]]
            [clj-kafka.zk :refer [brokers]]
            [clojure.tools.logging :as log]
            [clj-time.format :refer [unparse formatters]]
            [clojure.string :refer [join]]
            [backtype.storm.clojure :refer [emit-bolt! defbolt bolt ack!]]
            [cheshire.core :refer [generate-string]])
  (:gen-class))

(defbolt kafka-producer [] {:params [zk] :prepare true}
  [config context collector]
  (let [bs (join "," (map #(str (:host %) ":" (:port %))
                          (brokers {"zookeeper.connect" zk})))
        p (producer {"metadata.broker.list" bs
                     "serializer.class" "kafka.serializer.DefaultEncoder"
                     "partitioner.class" "kafka.producer.DefaultPartitioner"})]
    (bolt
      (execute [tuple]
               (let [event (update-in (tuple "event") [:created_at] #(unparse (formatters :rfc822) %))   
                     {:strs [type project]} tuple
                     json (generate-string {:type type :project project :event event})] 
                 (send-message p (message "events" (.getBytes (str type "_" project)) (.getBytes json))))
               (ack! collector tuple)))))
