(ns zoo-storm.json-scheme
  (:require [cheshire.core :refer [parse-string]])
  (:import 
           [backtype.storm.tuple Values Fields])
  (:gen-class
    :name zoo-storm.json-scheme
    :implements [backtype.storm.spout.Scheme]))

(defn -deserialize
  [_ bytes]
  (Values. (parse-string (apply str (map char bytes)) true)))

(defn -getOutputFields
  [_]
  (Fields. ["classification"]))

(defn new-json-scheme
  []
  (zoo-storm.json-scheme.))
