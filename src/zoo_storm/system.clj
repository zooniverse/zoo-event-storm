(ns zoo-storm.system
  [require [zoo-storm.event-topology :refer [run!]]])

(defn system
  []
  (let [env (System/getenv)]
    {:zookeeper (or (get env "ZK_URI") "33.33.33.10:2181")
     :postgres (or (get env "DATABASE_URL") "postgres://storm:storm@localhost:5433/events")
     :debug "true"
     :workers "2"}))

(defn start
  [system]
  (apply run! system))

(defn -main
  [& [debug workers]]
  (start (merge (system) {:debug debug :workers workers})))
