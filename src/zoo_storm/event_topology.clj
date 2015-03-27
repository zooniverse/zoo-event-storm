(ns zoo-storm.event-topology
  (:require [zoo-storm.spouts.kafka :refer [kafka-spout]]
            [zoo-storm.bolts.format-classifications :refer [format-classifications]]
            [zoo-storm.bolts.format-talk :refer [format-talk]]
            [zoo-storm.bolts.geocode :refer [geocode-event]]
            [zoo-storm.bolts.kafka :refer [kafka-format]]
            [zoo-storm.bolts.postgres :refer [to-postgres]]
            [backtype.storm [clojure :refer [topology spout-spec bolt-spec]] [config :refer :all]])
  (:import [backtype.storm LocalCluster StormSubmitter]
           [storm.kafka.bolt KafkaBolt])
  (:gen-class))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn gen-spouts
  [zk m topic]
  (let [s-name (str topic "-spout")]
    (assoc m s-name (spout-spec (kafka-spout zk topic (uuid)) :p 3))))

(defn topology-spouts
  [{:keys [topics zookeeper]}] 
  (reduce (partial gen-spouts zookeeper) {} topics))

(defn topology-bolts
  [{:keys [projects postgres topics]}]
  {"format-classification" (bolt-spec {"classifications-spout" :shuffle} 
                                      (format-classifications projects) :p 3)
   "format-talk" (bolt-spec {"talk_comments-spout" :shuffle}
                            (format-talk projects) :p 3)
   "geocode" (bolt-spec {"format-classification" :shuffle}
                        geocode-event :p 3)
   "format-kafka" (bolt-spec {"geocode" :shuffle "format-talk" :shuffle}
                             kafka-format :p 3)
   "write-to-kafka" (bolt-spec {"format-kafka" :shuffle} 
                               (KafkaBolt.) :p 3)
   "write-to-postgres" (bolt-spec {"geocode" :shuffle "format-talk" :shuffle}
                                  (to-postgres postgres) :p 3)})

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
