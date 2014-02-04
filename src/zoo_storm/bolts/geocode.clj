(ns zoo-storm.bolts.geocode
  (require [clj-http.client :as c]
           [backtype.storm.clojure :refer [defbolt emit-bolt! ack!]]))

(defn geocoder
  "Uses the Zooniverse Geocoding Service"
  [ip]
  (c/get (str "http://geo.zooniverse.org/" ip) {:accept :json :as :json}))

(defbolt geocode-event ["event"] {:params [geocode-fn]} [tuple collector]
  (let [location (geocode-fn (:ip tuple))
        tuple (assoc tuple :location location)]
    (emit-bolt! collector tuple :anchor tuple)
    (ack! collector tuple)))
