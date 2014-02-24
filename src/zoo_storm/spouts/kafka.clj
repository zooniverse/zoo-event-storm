(ns zoo-storm.spouts.kafka
  (:require [zoo-storm.json-scheme :refer [new-json-scheme]])
  (:import [storm.kafka KafkaSpout ZkHosts SpoutConfig StringScheme]
           [backtype.storm.spout SchemeAsMultiScheme Scheme]
           [backtype.storm.tuple Values Fields])
  (:gen-class))

(defn kafka-spout
  [zk-host topic & [client-id]]
  (let [brokers (ZkHosts. zk-host)
        kafka-config (SpoutConfig. brokers topic "" client-id)]   
    (set! (. kafka-config scheme) (SchemeAsMultiScheme. (new-json-scheme)))
    (KafkaSpout. kafka-config)))
