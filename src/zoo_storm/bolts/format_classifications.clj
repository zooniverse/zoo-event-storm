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

(defn to-annotation 
  [an]
  (not (or (contains? an :lang) 
           (contains? an :agent)
           (contains? an :user_agent))))

(defn get-user-agent
  [ans]
  (or (get-ans-key ans :agent)
      (get-ans-key ans :user_agent)))

(defn format-timestamp
  [classification]
  (let [format (formatters :date-time-no-ms)] 
    (parse format (-> (:timestamp classification)
                      (str/replace  #"\s(UTC|GMT)" "Z")
                      (str/replace  #"\s" "T")))))

(defn format-user-name
  [classification]
  (or (:user classification) "Not Logged In"))

(defn format-language
  [annotations]
  (let [lang (get-ans-key annotations :lang)]
    (cond
      (= "$DEFAULT" lang) "en-US"
      (= (count lang) 5) lang
      (= (count lang) 2) lang
      true "Unknown")))

(defn format-subjects
  [classification]
  (str/join "," (map :zooniverse_id (:subject classification))))

(defn format-user-id
  [classification]
  (or (:user_id classification) "Not Logged In"))

(defbolt format-classifications ["event" "type" "project"] {:params [projects]} [tuple collector]
  (let [cls (tuple "classification")
        ans (:annotations cls)]
    (when (some #{(:project cls)} projects)
      (emit-bolt! collector [{:user_id (format-user-id cls)
                              :user_ip (:user_ip cls)
                              :subjects (format-subjects cls)
                              :lang (format-language ans) 
                              :user_agent (get-user-agent ans)
                              :user_name (format-user-name cls)
                              :data (filter to-annotation ans) 
                              :created_at (format-timestamp cls)
                              :project (:project cls)} 
                             "classifications"
                             (:project cls)]
                  :anchor tuple))
    (ack! collector tuple)))
