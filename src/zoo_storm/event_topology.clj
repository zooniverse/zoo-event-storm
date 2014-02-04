(ns zoo-storm.event-topology
  (:require [zoo-storm.spouts.kafka :refer [kafka-spout]]
            [zoo-storm.bolts.parse-json :refer [parse-json]]
            [zoo-storm.bolts.gendercode :refer [gendercode-event code-name init-data]]
            [zoo-storm.bolts.geocode :refer [geocoder geocode-event]]
            [zoo-storm.bolts.kafka :refer [kafka-producer]]
            [backtype.storm [clojure :refer [topology spout-spec bolt-spec]] [config :refer :all]])
  (:import [backtype.storm LocalCluster])
  (:gen-class))

(def topology-spouts
  {"classifications-spout" (spout-spec (kafka-spout "33.33.33.10" "classifications"))})

(def topology-bolts
  {"parse-json" (bolt-spec {"classifications-spout" :shuffle} 
                           parse-json :p 2)
   "geocode" (bolt-spec {"parse-json" :shuffle}
                        geocode-event :p 2)
   "gendercode" (bolt-spec {"geocode" :shuffle}
                           gendercode-event :p 2)
   "write-to-kafka" (bolt-spec {"gendercode" :shuffle}
                               kafka-producer :p 2)})

(def event-topology
  (topology
    topology-spouts
    topology-bolts))

(defn run! [& {debug "debug" workers "workers" :or {debug "true" workers "2"}}]
  (doto (LocalCluster.)
    (.submitTopology "Event Topology"
                     {TOPOLOGY-DEBUG (Boolean/parseBoolean debug)
                      TOPOLOGY-WORKERS (Integer/parseInt workers)
                      TOPOLOGY-MAX-SPOUT-PENDING 200}
                     event-topology)))

(defn -main 
  [& args]
  (apply run! args))

(comment "to-http-stream" (bolt-spec {["gendercode"] :shuffle} to-http-stream :p 2)
   "to-database" (bolt-spec {["gendercode"] :shuffle} to-database))
