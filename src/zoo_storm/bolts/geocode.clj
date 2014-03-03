(ns zoo-storm.bolts.geocode
  (require [clj-geoip.core :refer [geoip-init lookup]]
           [clojure.tools.logging :as log]
           [backtype.storm.clojure :refer [defbolt bolt emit-bolt! ack!]])
  (:gen-class))

(defbolt geocode-event ["event" "type" "project"] {:prepare true} 
  [conf context collector]
  (let [geo-fn (geoip-init)] 
    (bolt
      (execute [{:strs [event type project] :as tuple}]
               (let [location (geo-fn (:user_ip event))
                     new-tuple (merge event {:country_name (:countryName location)
                                             :country_code (:countryCode location)
                                             :latitude (:latitude location)
                                             :longitude (:longitude location)
                                             :city_name (:city location)})]
                 (emit-bolt! collector [new-tuple type project] :anchor tuple)
                 (ack! collector tuple))))))
