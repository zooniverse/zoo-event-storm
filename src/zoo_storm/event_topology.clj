(ns zoo-storm.event-topology
  (:require [zoo-storm.spouts.kafka :refer [kafka-spout]]
            [zoo-storm.bolts.format-classifications :refer [format-classifications]]
            [zoo-storm.bolts.gendercode :refer [gendercode-event code-name init-data]]
            [zoo-storm.bolts.geocode :refer [geocode-event]]
            [zoo-storm.bolts.kafka :refer [kafka-format]]
            [zoo-storm.bolts.postgres :refer [to-postgres]]
            [backtype.storm [clojure :refer [topology spout-spec bolt-spec]] [config :refer :all]])
  (:import [backtype.storm LocalCluster StormSubmitter]
           [storm.kafka.bolt KafkaBolt])
  (:gen-class))

(defn gen-spouts
  [zk client-id m topic]
  (let [s-name (str topic "-spout")]
    (assoc m s-name (spout-spec (kafka-spout zk topic (or client-id "local-cluster")) :p 1))))

(defn topology-spouts
  [{:keys [topics zookeeper client-id]}] 
  (reduce (partial gen-spouts zookeeper client-id) {} topics))

(defn topology-bolts
  [{:keys [projects postgres topics]}]
  {"format-classification" (bolt-spec {"classifications-spout" :shuffle} 
                                      (format-classifications projects) :p 1)
   "geocode" (bolt-spec {"format-classification" :shuffle}
                        geocode-event :p 1)
   "gendercode" (bolt-spec {"geocode" :shuffle}
                           gendercode-event :p 1)
   "format-kafka" (bolt-spec {"gendercode" :shuffle}
                             kafka-format :p 1)
   "write-to-kafka" (bolt-spec {"format-kafka" :shuffle} 
                               (KafkaBolt.) :p 1)
   "write-to-postgres" (bolt-spec {"gendercode" ["type" "project"]}
                                  (to-postgres postgres) :p 1)})

(defn event-topology
  [conf]
  (topology
    (topology-spouts conf)
    (topology-bolts conf)))

(defn run-local! 
  [{:keys [kafka debug workers] :as conf}]
  (let [topology (event-topology conf)] 
    (doto (LocalCluster.)
      (.submitTopology "Event Topology"
                       {TOPOLOGY-DEBUG debug
                        "kafka.broker.properties"  {"metadata.broker.list" kafka} 
                        "topic" "events"
                        TOPOLOGY-WORKERS workers
                        TOPOLOGY-MAX-SPOUT-PENDING 200}
                       topology))))

(defn submit-topology! 
  [{:keys [kafka debug workers] :as conf} name]
  (StormSubmitter/submitTopology
    name
    {TOPOLOGY-DEBUG false
     TOPOLOGY-WORKERS workers
     TOPOLOGY-MESSAGE-TIMEOUT-SECS 60
     "kafka.broker.properties"  {"metadata.broker.list" kafka} 
     "topic" "events"
     TOPOLOGY-MAX-SPOUT-PENDING 200}
    (event-topology (merge conf {:client-id name}))))
