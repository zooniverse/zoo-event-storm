(ns zoo-storm.json-scheme
  (:require [cheshire.core :refer [parse-string]])
  (:import [java.util List]
           [backtype.storm.tuple Values Fields])
  (:gen-class
    :name zoo-storm.json-scheme
    :implements [backtype.storm.spout.Scheme]))

(defn -deserialize
  [_ bytes]
  [(parse-string (apply str (map char bytes)) true)])

(defn -getOutputFields
  [_]
  (Fields. ["classifications"]))

(defn new-json-scheme
  []
  (zoo-storm.json-scheme.))
