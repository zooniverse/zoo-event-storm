(ns zoo-storm.bolts.geocode
  (require [clj-http.client :as c]
           [clj-geoip.core :refer [geoip-init lookup]]
           [backtype.storm.clojure :refer [defbolt emit-bolt! ack!]]))

(defn geocoder
  []
  (geoip-init)
  (fn [ip]
    (lookup ip)))

(defbolt geocode-event ["event"] {:params [geo-fn]} [tuple collector]
  (let [location (geo-fn (:ip tuple))
        tuple (assoc tuple :location {:country_name (:countryName location)
                                      :country_code (:countryCode location)
                                      :city (:city location)})]
    (emit-bolt! collector tuple :anchor tuple)
    (ack! collector tuple)))
