(ns zoo-storm.bolts.geocode
  (require [clj-geoip.core :refer [geoip-init lookup]]
           [clojure.tools.logging :as log]
           [backtype.storm.clojure :refer [defbolt bolt emit-bolt! ack!]])
  (:gen-class))

(defn geocoder
  []
  (geoip-init)
  (fn [ip]
    (lookup ip)))

(defbolt geocode-event ["event"] {:prepare true} 
  [conf context collector]
  (let [geo-fn (geocoder)] 
    (bolt
      (execute [tuple]
               (let [event (tuple "event")
                     location (geo-fn (:ip event))
                     new-tuple (assoc event :location {:country_name (:countryName location)
                                                       :country_code (:countryCode location)
                                                       :city (:city location)})]
                 (emit-bolt! collector [new-tuple] :anchor tuple)
                 (ack! collector tuple))))))
