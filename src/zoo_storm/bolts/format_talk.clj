(ns zoo-storm.bolts.format-talk
  (:require [backtype.storm.clojure :refer [bolt emit-bolt! ack! defbolt]]
            [clojure.string :as str]
            [clj-time.format :refer [formatters parse]]
            [clojure.tools.logging :as log]
            [clojure.java.io :refer :all])
  (:gen-class))

(defn format-created_at
  [timestamp]
  (parse (formatters :date-time-no-ms) timestamp))

(defbolt format-talk ["event" "type" "project"] [tuple collector]
  (let [talk-comment (tuple "json")
        data-fields [:id
                     :focus
                     :board
                     :body
                     :tags
                     :user_zooniverse_id
                     :zooniverse_id]]
    (emit-bolt! collector [(-> (apply dissoc talk-comment data-fields)
                               (merge {:data (select-keys talk-comment data-fields)})
                               (update-in [:created_at] format-created_at))
                           "talk_comments"
                           (:project talk-comment)]
                :anchor tuple)
    (ack! collector tuple)))
