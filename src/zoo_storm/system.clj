(ns zoo-storm.system
  (:require [zoo-storm.event-topology :refer [run-local! submit-topology!]])
  (:gen-class))

(defn system
  []
  (let [env (System/getenv)]
    {:zookeeper (or (get env "ZK_URI") "33.33.33.10:2181")
     :postgres (or (get env "DATABASE_URL") "postgres://storm:storm@localhost:5433/events")
     :topics ["classifications"]
     :debug true
     :workers 2}))

(defn start
  ([system]
   (println system)
   (run-local! system))
  ([system name]
   (submit-topology! system name)))

(defn stop
  [system]
  {})

(defn -main
  [& [postgres zookeeper name]]
  (if name
    (start (merge (system) (into {} (filter second {:postgres postgres :zookeeper zookeeper :projects ["wise"]}))) name)
    (start (merge (system) (into {} (filter second {:postgres postgres :zookeeper zookeeper}))))))
