(ns zoo-storm.spouts.kafka
  (:import [storm.kafka KafkaSpout ZkHosts SpoutConfig]
           [backtype.storm.spout SchemeAsMultiScheme]))

(defn kafka-spout
  [zk-host topic & [client-id]]
  (let [brokers (ZkHosts. zk-host)
        kafka-config (SpoutConfig. brokers topic client-id)]
    (set! (. kafka-config scheme) (SchemeAsMultiScheme.))
    (KafkaSpout. kafka-config)))
