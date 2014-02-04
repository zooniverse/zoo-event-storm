(ns zoo-storm.event-topology
  (:require [zoo-storm.spouts.kafka :refer kafka-spout]
            [zoo-storm.bolts.geocode :refer [geocoder geocode-event]]
    [backtype.storm [clojure :refer [topology spout-spec bolt-spec]] [config :refer :all]])
  (:import [backtype.storm LocalCluster]))

(def topology-spouts
  {"classifications-spout" (spout-spec (kafka-spout "classifications"))})

(def topology-bolts
  {"geocode" (bolt-spec {["classifications-spout"] :shuffle}
                        (geocode-event (geocoder)) :p 2)
   "gendercode" (bolt-spec {["geocode"] :shuffle}
                          gendercode-event :p 2)
   "to-http-stream" (bolt-spec {["gendercode"] :shuffle}
                               to-http-stream :p 2)
   "to-database" (bolt-spec {["gendercode"] :shuffle}
                            to-database)})

(def event-topology
 code  (topology
    topology-spouts
    topology-bolts))

(defn run! [& {debug "debug" workers "workers" :or {debug "true" workers "2"}}]
  (doto (LocalCluster.)
    (.submitTopology "Event Topology"
                     {TOPOLOGY-DEBG (Boolean/parseBoolean debug)
                      TOPOLOGY-WORKERS (Integer/parseInt workers)
                      TOPOLOGY-MAX-SPOUT-PENDING 200}
                     event-topology)))
