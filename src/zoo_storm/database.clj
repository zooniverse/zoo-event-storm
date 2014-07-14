(ns zoo-storm.database
  (:require [korma.core :refer :all]
            [korma.db :refer [postgres create-db with-db]]
            [paneer.core :as p]
            [paneer.db :as pdb]
            [clojure.string :refer [split]]))

(defn- table-exists?
  [db table-name]
  (not (empty? (with-db db
                 (select "information_schema.columns"
                         (where {:table_name table-name}))))))


(defn create-table-if-not-exists
  [db tbl]
  (if-not (table-exists? db tbl)
    (-> (p/create*)
        (p/table tbl)
        (p/column :id :bigserial "PRIMARY KEY")
        (p/varchar :user_id 24)
        (p/varchar :user_ip 15)
        (p/text :lang)
        (p/text :project)
        (p/text :user_agent)
        (p/text :user_name)
        (p/text :subjects)
        (p/column :data :json)
        (p/timestamp :created_at)
        (p/varchar :country_code 2)
        (p/varchar :country_name 50)
        (p/varchar :city_name 50)
        (p/float :latitude)
        (p/float :longitude)
        (pdb/execute :db db))))

(defn- uri-to-db-map
  [uri]
  (let [uri (java.net.URI. uri)
        [username password] (split (.getUserInfo uri) #":")]
    {:db (apply str (drop 1 (.getPath uri)))
     :user username
     :password password
     :host (.getHost uri)
     :port (.getPort uri)}))

(defn create-db-connection
  [pg-uri]
  (-> (uri-to-db-map pg-uri) postgres create-db))
