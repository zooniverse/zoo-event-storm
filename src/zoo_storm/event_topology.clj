(ns zoo-storm.event-topology
  (:require [zoo-storm.spouts.kafka :refer [kafka-spout]]
            [zoo-storm.bolts.format-classifications :refer [format-classifications]]
            [zoo-storm.bolts.gendercode :refer [gendercode-event code-name init-data]]
            [zoo-storm.bolts.geocode :refer [geocoder geocode-event]]
            [zoo-storm.bolts.kafka :refer [kafka-producer]]
            [zoo-storm.bolts.postgres :refer [to-postgres]]
            [backtype.storm [clojure :refer [topology spout-spec bolt-spec]] [config :refer :all]])
  (:import [backtype.storm LocalCluster])
  (:gen-class))

(defn topology-spouts
  [conf]
  {"classifications-spout" (spout-spec (kafka-spout (:zookeeper conf) "classifications_*"))})

(defn topology-bolts
  [conf]
  {"format-classification" (bolt-spec {"classifications-spout" :shuffle} 
                           format-classifications :p 2)
   "geocode" (bolt-spec {"format-classification" :shuffle}
                        geocode-event :p 2)
   "gendercode" (bolt-spec {"geocode" :shuffle}
                           gendercode-event :p 2)
   "write-to-kafka" (bolt-spec {"gendercode" :shuffle}
                               (kafka-producer :zookeeper conf) :p 2)
   "write-to-postgres" (bolt-spec {"gendercode" ["type" "project"]}
                                  (to-postgres (:postgres conf)))})

(defn event-topology
  [conf]
  (topology
    (topology-spouts conf)
    (topology-bolts conf)))

(defn run! [& {debug "debug" workers "workers" :or {debug "true" workers "2"} :as conf}]
  (let [topology (event-topology conf)] 
    (doto (LocalCluster.)
      (.submitTopology "Event Topology"
                       {TOPOLOGY-DEBUG (Boolean/parseBoolean debug)
                        TOPOLOGY-WORKERS (Integer/parseInt workers)
                        TOPOLOGY-MAX-SPOUT-PENDING 200}
                       event-topology))))

(comment "to-http-stream" (bolt-spec {["gendercode"] :shuffle} to-http-stream :p 2)
         "to-database" (bolt-spec {["gendercode"] :shuffle} to-database))
