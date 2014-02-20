(ns zoo-storm.spouts.kafka
  (:require [cheshire.core :refer [parse-string]])
  (:import [storm.kafka KafkaSpout ZkHosts SpoutConfig StringScheme]
           [backtype.storm.spout SchemeAsMultiScheme Scheme]
           [backtype.storm.tuple Values Fields])
  (:gen-class))

(defn kafka-spout
  [zk-host topic & [client-id]]
  (let [brokers (ZkHosts. zk-host)
        kafka-config (SpoutConfig. brokers topic "" client-id)
        json-scheme (reify Scheme
                      (deserialize [this bytes]
                        (Values. (parse-string (apply str (map char bytes)) true)))
                      (getOutputFields [this]
                        (Fields. "str")))]
    (set! (. kafka-config scheme) (SchemeAsMultiScheme. json-scheme))
    (KafkaSpout. kafka-config)))
