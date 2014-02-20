(ns zoo-storm.bolts.format-classifications
  (:require [backtype.storm.clojure :refer [bolt emit-bolt! ack! defbolt]]
            [clojure.string :as str]
            [clj-time.format :refer [formatters parse]]
            [clojure.java.io :refer :all])
  (:gen-class))

(defbolt format-classifications ["event" "type" "project"] [tuple collector]
  (let [cls (tuple "classification")
        ans (:annotations cls)
        form (formatters :rfc822)]
    (emit-bolt! collector [{:classification_id (:_id cls)
                            :user_id (or (:user_id cls) "Not Logged In")
                            :user_ip (:user_ip cls)
                            :user_agent (-> (filter #(contains? % :user_agent) ans) 
                                            first 
                                            second) 
                            :user_name (:user_name cls)
                            :data ans
                            :created_at (->> (filter #(contains? % :finished_at ans))
                                            first
                                            second
                                             (parse form))}
                           "classification"
                           (:project_name cls)]
                :anchor tuple)
    (ack! collector tuple)))
