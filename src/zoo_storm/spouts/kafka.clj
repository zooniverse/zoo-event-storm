(ns zoo-storm.spouts.kafka
  (:import [storm.kafka KafkaSpout ZkHosts SpoutConfig StringScheme]
           [backtype.storm.spout SchemeAsMultiScheme])
  (:gen-class))

(defn kafka-spout
  [zk-host topic & [client-id]]
  (let [brokers (ZkHosts. zk-host)
        kafka-config (SpoutConfig. brokers topic "" client-id)]
    (set! (. kafka-config scheme) (SchemeAsMultiScheme. (StringScheme.)))
    (KafkaSpout. kafka-config)))
