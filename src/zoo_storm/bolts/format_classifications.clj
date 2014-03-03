(ns zoo-storm.bolts.format-classifications
  (:require [backtype.storm.clojure :refer [bolt emit-bolt! ack! defbolt]]
            [clojure.string :as str]
            [clj-time.format :refer [formatters parse]]
            [clojure.tools.logging :as log]
            [clojure.java.io :refer :all])
  (:gen-class))

(defbolt format-classifications ["event" "type" "project"] [tuple collector]
  (let [cls (tuple "classification")
        ans (:annotations cls)  
        form (formatters :date-time-no-ms)
        t (str/replace (str/replace (:timestamp cls) #"\s(UTC|GMT)" "Z")
                       #"\s" "T")]
    (emit-bolt! collector [{:data_id (:id cls)

                            :user_id (or (:user_id cls) "Not Logged In")
                            :user_ip (:user_ip cls)
                            :subjects (str/join "," (:subjects cls))
                            :lang (-> (filter #(contains? % :user_agent) ans)
                                      first
                                      :lang)
                            :user_agent (-> (filter #(contains? % :user_agent) ans) 
                                            first 
                                            :user_agent)
                            :user_name (or (:user cls) "Not Logged In")
                            :data ans
                            :created_at (parse form t)}
                           "classifications"
                           (:project cls)]
                :anchor tuple)
    (ack! collector tuple)))
