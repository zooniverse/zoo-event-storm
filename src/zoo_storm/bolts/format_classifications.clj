(ns zoo-storm.bolts.format-classifications
  (:require [backtype.storm.clojure :refer [bolt emit-bolt! ack! defbolt]]
            [clojure.string :as str]
            [clj-time.format :refer [formatters parse]]
            [clojure.tools.logging :as log]
            [clojure.java.io :refer :all])
  (:gen-class))

(defbolt format-classifications ["event" "type" "project"] [tuple collector]
  (let [cls (tuple "classification")
        ans (cls "annotations")
        form (formatters :rfc822)]
    (log/info cls)
    (log/info ans)
    (log/info (first (filter #(contains? % :finished_at) ans)))
    (emit-bolt! collector [{:classification_id (:_id cls)
                            :user_id (or (:user_id cls) "Not Logged In")
                            :user_ip (:user_ip cls)
                            :user_agent (-> (filter #(contains? % :user_agent) ans) 
                                            first 
                                            second) 
                            :user_name (or (:user_name cls) "Not Logged In")
                            :data ans
                            :created_at (->> (filter #(contains? % :finished_at) ans)
                                            first
                                            second
                                             (parse form))}
                           "classification"
                           (:project_name cls)]
                :anchor tuple)
    (ack! collector tuple)))
