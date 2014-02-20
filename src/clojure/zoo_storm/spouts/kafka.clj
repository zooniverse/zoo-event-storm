(ns zoo-storm.spouts.kafka
  (:import [storm.kafka KafkaSpout ZkHosts SpoutConfig StringScheme]
           [backtype.storm.spout SchemeAsMultiScheme Scheme]
           [backtype.storm.tuple Values Fields]
           [zoo_storm JsonClojureScheme])
  (:gen-class))

(defn kafka-spout
  [zk-host topic & [client-id]]
  (let [brokers (ZkHosts. zk-host)
        kafka-config (SpoutConfig. brokers topic "" client-id)]   
    (set! (. kafka-config scheme) (SchemeAsMultiScheme. (JsonClojureScheme.)))
    (KafkaSpout. kafka-config)))
