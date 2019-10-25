(ns ks-talk.simple-jackdaw
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
      (j/map-values (fn [v]
                      (-> (select-keys v [:id :timestamp :payment-method])
                          (assoc :amount-cents (int (* (:amount v) 100))))))
      (j/filter (fn [v]
                  (= (:payment-method v) :direct-debit)))
      (j/to (->topic-config "direct-debit-transactions"))))


(defn start! []
  (let [builder (j/streams-builder)]
    (doto (j/kafka-streams (topology builder) config)
      (j/start))))