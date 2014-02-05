(ns zoo-storm.bolts.postgres
  (:require [clojure.java.jdbc :as j]
            [clojure.java.jdbc.sql :as s]
            [backtype.storm.clojure :refer [defbolt emit-bolt! ack! bolt]])
  (:gen-class))

(defbolt to-postgres ["event"] {:params [pg-uri] :prepare true}
  [conf context collector]
  (let [insert (partial j/insert! pg-uri)
        batch (atom [])]
    (bolt
      (execute [tuple]
               (swap! batch conj (tuple "event"))
               (if (= 10 (count @batch))
                 (do
                   (apply insert :events @batch)
                   (emit-bolt! collector 
                               [(str "Saved " 
                                     (count @batch) 
                                     "events")] 
                               :anchor tuple)
                   (reset! batch []))
                 (emit-bolt! collector ["batched"] :anchor tuple))
               (ack! collector tuple)))))