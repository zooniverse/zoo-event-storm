(ns zoo-storm.bolts.postgres
  (:require [clojure.java.jdbc :as j]
            [clojure.java.jdbc.sql :as s]
            [backtype.storm.clojure :refer [defbolt emit-bolt! ack! bolt]])
  (:gen-class))

(defn to-database-map
  "Formats the storm tuple to the database schema. It expects a table called
  events in the database with the following schema
  (id bigserial PRIMARY KEY, 
   country_code varchar(2),
   country_name varchar(256),
   city varchar(256),
   gender varchar(1),
   male_prob float,
   female_prob float,
   classification_id varchar(24),
   user_id varchar(25),
   created_at datetime,
   data json)"
  [{:strs [event type]}]
  {})

(defbolt to-postgres ["action"] {:params [pg-uri] :prepare true}
  [conf context collector]
  (let [insert (partial j/insert! pg-uri)
        batch (atom {})]
    (bolt
      (execute [{:strs [event type project] :as tuple}]
               (swap! batch update-in [type] conj [event type])
               (if (= 10 (count (@batch type)))
                 (do
                   (apply insert 
                          (str type "-events") 
                          (map to-database-map (@batch type)))
                   (emit-bolt! collector 
                               [(str "Saved " 
                                     (count @batch) 
                                     " "
                                     type
                                     " events")] 
                               :anchor tuple)
                   (reset! batch []))
                 (emit-bolt! collector ["batched"] :anchor tuple))
               (ack! collector tuple)))))
