(ns zoo-storm.bolts.postgres
  (:require [korma.core :refer :all]
            [korma.db :refer [postgres create-db]]
            [paneer.core :as p]
            [paneer.db :as pdb]
            [clojure.string :refer [split]]
            [backtype.storm.clojure :refer [defbolt emit-bolt! ack! bolt]])
  (:gen-class))

(def batch-queue-limit 10)

(defn table-exists?
  [table-name]
  (not (empty? (select :INFORMATION_SCHEMA.COLUMNS
                       (where {:table_name table-name})))))

(def table-exists?-memo (memoize table-exists?))

(defn create-table-if-not-exists
  [tbl]
  (if-not (table-exists?-memo tbl)
    (-> (p/create*)
        (p/table tbl)
        (p/column :id :bigserial "PRIMARY KEY")
        (p/varchar :classification_id 24)
        (p/varchar :user_id 24)
        (p/varchar :user_ip 15)
        (p/text :user_agent)
        (p/text :user_name)
        (p/column :data :json)
        (p/timestamp :created_at)
        (p/varchar :country_code 2)
        (p/varchar :contry_name 50)
        (p/varchar :city_name 50)
        (p/float :latitude)
        (p/float :longitude)
        (p/varchar :gender 1)
        (p/float :male)
        (p/float :female)
        (pdb/execute))))

(defn- uri-to-db-map
  [uri]
  (let [uri (java.net.URI. uri)
        [username password] (split (.getUserInfo uri) #":")]
    {:db (apply str (drop 1 (.getPath uri)))
     :user username
     :password password
     :host (.getHost uri)
     :port (.getPort uri)}))

(defbolt to-postgres ["action"] {:params [pg-uri] :prepare true}
  [conf context collector]
  (let [db (-> (uri-to-db-map pg-uri) postgres create-db)
        batch (atom {})]
    (bolt
      (execute [{:strs [event type project] :as tuple}]
               (let [key (keyword (str type "-" project))
                     tbl-name (str "events_" type "_" project)]
                 (swap! batch update-in [key] conj event)
                 (if (= batch-queue-limit (count (@batch key)))
                   (do
                     (create-table-if-not-exists tbl-name)
                     (insert (keyword tbl-name)
                             (values (@batch key)))
                     (emit-bolt! collector 
                                 [(str "Saved " 
                                       (count (@batch key)) 
                                       " "
                                       type
                                       " "
                                       project
                                       " events")] 
                                 :anchor tuple)
                     (swap! batch assoc key []))
                   (emit-bolt! collector ["batched"] :anchor tuple)))
               (ack! collector tuple)))))
