(ns zoo-storm.bolts.kafka
  (:require [clj-time.format :refer [unparse formatters]]
            [clojure.string :refer [join]]
            [backtype.storm.clojure :refer [emit-bolt! defbolt bolt ack!]]
            [cheshire.core :refer [generate-string]])
  (:gen-class))

(defbolt kafka-format ["key" "message"] [{:strs [event type project] :as tuple} collector] 
  (let [event (update-in event [:created_at] #(unparse (formatters :rfc822) %))   
        json (.getBytes (generate-string {:type type :project project :event event}))] 
    (emit-bolt! collector [(.getBytes (str type "_" project)) json] :anchor tuple)
    (ack! collector tuple)))
