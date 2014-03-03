(ns zoo-storm.event-topology
  (:require [zoo-storm.spouts.kafka :refer [kafka-spout]]
            [zoo-storm.bolts.format-classifications :refer [format-classifications]]
            [zoo-storm.bolts.gendercode :refer [gendercode-event code-name init-data]]
            [zoo-storm.bolts.geocode :refer [geocode-event]]
            [zoo-storm.bolts.kafka :refer [kafka-producer]]
            [zoo-storm.bolts.postgres :refer [to-postgres]]
            [backtype.storm [clojure :refer [topology spout-spec bolt-spec]] [config :refer :all]])
  (:import [backtype.storm LocalCluster StormSubmitter])
  (:gen-class))

(defn gen-spouts
  [zk m topic]
  (let [s-name (str topic "-spout")]
    (assoc m s-name (spout-spec (kafka-spout zk topic)))))

(defn topology-spouts
  [{:keys [topics zookeeper]}]
  (reduce (partial gen-spouts zookeeper) {} topics))

(defn topology-bolts
  [{:keys [zookeeper postgres topics]}]
  (let [spouts (reduce #(assoc %1 (str %2 "-spout") :shuffle) {} topics)] 
    {"format-classification" (bolt-spec spouts
                           format-classifications :p 5)
   "geocode" (bolt-spec {"format-classification" :shuffle}
                        geocode-event :p 2)
   "gendercode" (bolt-spec {"geocode" :shuffle}
                           gendercode-event :p 2)
   "write-to-kafka" (bolt-spec {"gendercode" :shuffle}
                               (kafka-producer zookeeper) :p 2)
   "write-to-postgres" (bolt-spec {"gendercode" ["type" "project"]}
                                  (to-postgres postgres))}))

(defn event-topology
  [conf]
  (topology
    (topology-spouts conf)
    (topology-bolts conf)))

(defn run-local! [{debug :debug workers :workers :as conf}]
  (let [topology (event-topology conf)] 
    (doto (LocalCluster.)
      (.submitTopology "Event Topology"
                       {TOPOLOGY-DEBUG debug
                        TOPOLOGY-WORKERS workers
                        TOPOLOGY-MAX-SPOUT-PENDING 200}
                       topology))))

(defn submit-topology! [{debug :debug workers :workers :as conf} name]
  (StormSubmitter/submitTopology
    name
    {TOPOLOGY-DEBUG debug
     TOPOLOGY-WORKERS workers}
    (event-topology conf)))
