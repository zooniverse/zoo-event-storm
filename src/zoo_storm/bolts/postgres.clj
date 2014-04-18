(ns zoo-storm.bolts.postgres
  (:require [korma.core :refer :all]
            [korma.db :refer [with-db]]
            [cheshire.core :refer [generate-string]]
            [clojure.tools.logging :as log]
            [pg-json.core :refer :all]
            [zoo-storm.database :refer :all]
            [backtype.storm.clojure :refer [defbolt emit-bolt! ack! bolt]])
  (:import java.sql.Timestamp)
  (:gen-class))

(def batch-queue-limit 25)

(defn- to-sql-time
  [dt]
  (java.sql.Timestamp. (.getMillis dt)))

(defbolt to-postgres [] {:params [pg-uri] :prepare true}
  [conf context collector]
  (let [db (create-db-connection pg-uri)
        batch (atom {})
        transformer (comp #(update-in % [:created_at] to-sql-time)
                          #(update-in % [:data] to-json-column))]
    (bolt
      (execute [{:strs [event type project] :as tuple}]
               (ack! collector tuple)    

               (let [tbl-name (str "events_" type "_" project)
                     project-batch (@batch key)]
                 (swap! batch update-in [key] assoc (:data_id event) event)
                 (when (= batch-queue-limit (count project-batch))
                   (let [existing-ids (with-db db
                                        (select tbl-name
                                                (where {:data_id [in (keys project-batch)]})))
                         data (->> (apply dissoc project-batch existing-ids)
                                   values
                                   (mapv transformer))]
                     (with-db db
                       (insert tbl-name
                               (values data)))
                     (swap! batch dissoc key))))))))
