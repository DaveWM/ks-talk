(ns ks-talk.02-simple-jackdaw
  (:require [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse]))


(def config
  {"application.id" "repayments-processor-jackdaw"
   "bootstrap.servers" "localhost:9092"})

(defn ->topic-config [topic-name]
  {:topic-name topic-name
   :partition-count 1
   :replication-factor 1
   :topic-config {}
   :key-serde (jse/serde)
   :value-serde (jse/serde)})

(defn topology [builder]
  (-> (j/kstream builder (->topic-config "repayments"))
      (j/map (fn [[k v]]
               [k (-> (select-keys v [:id :timestamp :payment-method])
                      (assoc :amount-cents (int (* (:amount v) 100))))]))
      (j/filter (fn [v]
                  (= (:payment-method v) :direct-debit)))
      (j/to (->topic-config "direct-debit-transactions"))))


(defn start! []
  (let [builder (j/streams-builder)]
    (topology builder)
    (doto (j/kafka-streams builder config)
      (j/start))))