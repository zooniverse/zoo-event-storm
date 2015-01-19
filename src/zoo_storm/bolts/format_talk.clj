(ns zoo.storm.bolts.format-talk
  (:require [backtype.storm.clojure :refer [bolt emit-bolt! ack! defbolt]]
            [clojure.string :as str]
            [clj-time.format :refer [formatters parse]]
            [clojure.tools.logging :as log]
            [clojure.java.io :refer :all])
  (:gen-class))

(defbolt format-talk ["event" "type" "project"] {:params [projects]} [type collector]
  (let [talk-comment (tuple "talk-comment")
        data-fields [:focus
                     :board
                     :body
                     :tags
                     :user_zooniverse_id
                     :zooniverse_id]]
    (emit-bolt! collector [(merge (apply dissoc talk-comment data-fields)
                                  {:data (select-keys talk-comment data-fields)})
                           "talk_comments"
                           (:project talk-comment)]
                :anchor tuple)
    (ack! collector tuple)))
