(ns zoo-storm.bolts.format-classifications
  (:require [backtype.storm.clojure :refer [bolt emit-bolt! ack! defbolt]]
            [clojure.string :as str]
            [clj-time.format :refer [formatters parse]]
            [clojure.tools.logging :as log]
            [clojure.java.io :refer :all])
  (:gen-class))

(defn get-ans-key
  [ans k]
  (-> (filter #(contains? % k) ans) first k))

(defbolt format-classifications ["event" "type" "project"] {:params [projects]} [tuple collector]
  (let [cls (tuple "classification")
        ans (:annotations cls)  
        form (formatters :date-time-no-ms)
        t (str/replace (str/replace (:timestamp cls) #"\s(UTC|GMT)" "Z")
                       #"\s" "T")
        user-agent (or (get-ans-key ans :agent)
                       (get-ans-key ans :user_agent))
        data (filter #(not (or (contains? % :lang) 
                               (contains? % :agent)
                               (contains? % :user_agent))) 
                     ans)]
    (when (some #{(:project cls)} projects)
      (emit-bolt! collector [{:user_id (or (:user_id cls) "Not Logged In")
                              :user_ip (:user_ip cls)
                              :subjects (str/join "," (map :zooniverse_id (:subject cls)))
                              :lang (get-ans-key ans :lang)
                              :user_agent user-agent
                              :user_name (or (:user cls) "Not Logged In")
                              :data data
                              :created_at (parse form t)}
                             "classifications"
                             (:project cls)]
                  :anchor tuple))
    (ack! collector tuple)))
