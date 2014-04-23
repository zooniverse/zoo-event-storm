(ns zoo-storm.bolts.format-classifications
  (:require [backtype.storm.clojure :refer [bolt emit-bolt! ack! defbolt]]
            [clojure.string :as str]
            [clj-time.format :refer [formatters parse]]
            [clojure.tools.logging :as log]
            [clojure.java.io :refer :all])
  (:gen-class))

(defbolt format-classifications ["event" "type" "project"] {:params [projects]} [tuple collector]
  (let [cls (tuple "classification")
        ans (:annotations cls)  
        form (formatters :date-time-no-ms)
        t (str/replace (str/replace (:timestamp cls) #"\s(UTC|GMT)" "Z")
                       #"\s" "T")]
    (when (some #{(:project cls)} projects)
      (emit-bolt! collector [{:user_id (or (:user_id cls) "Not Logged In")
                              :user_ip (:user_ip cls)
                              :subjects (str/join "," (:zooniverse_id (:subject cls)))
                              :lang (-> (filter #(contains? % :lang) ans) first
                                        :lang)
                              :user_agent (-> (filter #(contains? % :agent) ans) first :agent)
                              :user_name (or (:user cls) "Not Logged In")
                              :data (filter #(not (or (contains? % :lang) (contains? % :agent))) ans)
                              :created_at (parse form t)}
                             "classifications"
                             (:project cls)]
                  :anchor tuple))
    (ack! collector tuple)))
