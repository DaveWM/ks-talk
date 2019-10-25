(ns ks-talk.01-simple
  (:require [clojure.data.json :as json])
  (:import (org.apache.kafka.streams StreamsBuilder StreamsConfig KafkaStreams)
           (org.apache.kafka.streams.kstream KeyValueMapper Predicate Consumed Produced)
           (java.nio.charset StandardCharsets)
           (org.apache.kafka.common.serialization Serializer Deserializer Serdes)))


(defn bytes-to-string
  [data]
  (String. data StandardCharsets/UTF_8))

(defn string-to-bytes
  [data]
  (.getBytes ^String data StandardCharsets/UTF_8))

(def json-serializer
  (reify
    Serializer
    (close [_])
    (configure [_ _ _])
    (serialize [_ _ data]
      (when data
        (-> (json/write-str data)
            (string-to-bytes))))))

(def json-deserializer
  (reify
    Deserializer
    (close [_])
    (configure [_ _ _])
    (deserialize [_ _topic data]
      (when data
        (-> (bytes-to-string data)
            (json/read-str :key-fn keyword))))))

(def json-serde
  (Serdes/serdeFrom json-serializer json-deserializer))

(def config
  (StreamsConfig.
   {StreamsConfig/APPLICATION_ID_CONFIG, "repayments-processor"
    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "broker:9092"
    StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))
    StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))}))

(defn topology [^StreamsBuilder builder]
  (-> (.stream builder "repayments" (Consumed/with (Serdes/String) json-serde))
      (.map (reify KeyValueMapper
                    (apply [this k v]
                      (-> (select-keys v [:id :timestamp :payment-method])
                          (assoc :amount-cents (int (* (:amount v) 100)))))))
      (.filter (reify Predicate
                 (test [this k v]
                   (= (:payment-method v) :direct-debit))))
      (.to "direct-debit-transactions" (Produced/with (Serdes/String) json-serde))))


(defn start! []
  (let [builder (StreamsBuilder.)]
    (doto (KafkaStreams. (.build (topology builder)) config)
      (.start))))


