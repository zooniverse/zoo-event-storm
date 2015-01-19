(ns zoo-storm.system
  (:require [zoo-storm.event-topology :refer [run-local! submit-topology!]]
            [zoo-storm.database :refer :all]
            [clojure.math.combinatorics :refer [cartesian-product]])
  (:gen-class))

(defn system
  []
  {:zookeeper ""
   :postgres "" 
   :kafka ""
   :topics ["classifications", "talk_comments"]
   :debug true
   :workers 1})

(defn create-tables
  [{:keys [topics postgres]}]
  (let [db (create-db-connection postgres)]
    (doseq [topic topics]
      (create-table-if-not-exists db (str "events_" topic)))))

(defn start
  ([system]
   (create-tables system)
   (run-local! system))
  ([system name]
   (create-tables system)
   (submit-topology! system name)))

(defn stop
  [system]
  {})

(defn -main
  [& [postgres zookeeper kafka name]]
  (let [args {:postgres postgres 
              :zookeeper zookeeper
              :kafka kafka}
        system (merge (system) args)] 
    (if name
      (start system name)
      (start system))))
