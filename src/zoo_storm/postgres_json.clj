(ns zoo-storm.postgres-json
  (:require [cheshire.core :refer [generate-string parse-string]])
  (:import [org.postgresql.util PGobject]))

(defprotocol JSONstorable
  (to-json-column [this]))

(defprotocol JSONretrievable
  (from-json-column [this]))

(extend-type clojure.lang.IPersistentMap
  JSONstorable
  (to-json-column [this]
    (doto (PGobject.)
      (.setType "json")
      (.setValue (generate-string this)))))

(extend-type clojure.lang.IPersistentVector
  JSONstorable
  (to-json-column [this]
    (doto (PGobject.)
      (.setType "json")
      (.setValue (generate-string this)))))

(extend-type org.postgresql.util.PGobject
  JSONretrievable
  (from-json-column [this]
    (when (= (.getType this) "json")
      (parse-string (.getValue this)))))
