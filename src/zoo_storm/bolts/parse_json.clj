(ns zoo-storm.bolts.parse-json
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [backtype.storm.clojure :refer [defbolt emit-bolt! ack!]]))

(defbolt parse-json ["event"] [tuple collector]
  (emit-bolt! collector [(parse-string (.getString tuple 0) true)] :anchor tuple)
  (ack! collector tuple))
