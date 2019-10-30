(ns ks-talk.03-simple-transducers
  (:require [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse])
  (:import (org.apache.kafka.streams.kstream Transformer)
           (org.apache.kafka.streams.processor ProcessorContext)))

;; see https://github.com/DaveWM/willa/blob/master/src/willa/streams.clj#L110
(deftype TransducerTransformer [xform ^{:volatile-mutable true} context]
  Transformer
  (init [_ c]
    (set! context c))
  (transform [_ k v]
    (let [rf (fn
               ([context] context)
               ([^ProcessorContext context [k v]]
                (.forward context k v)
                (.commit context)
                context))]
      ((xform rf) context [k v]))
    nil)
  (close [_]))


(defn transduce-stream [kstream xform]
  (j/transform kstream #(TransducerTransformer. xform nil)))


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

(def process-repayments-xf
  (comp
   (map (fn [[k v]]
          [k (-> (select-keys v [:id :timestamp :payment-method])
                 (assoc :amount-cents (int (* (:amount v) 100))))]))
   (filter (fn [[k v]]
             (= (:payment-method v) :direct-debit)))))

(defn topology [builder]
  (-> (j/kstream builder (->topic-config "repayments"))
      (transduce-stream process-repayments-xf)
      (j/to (->topic-config "direct-debit-transactions"))))

(defn start! []
  (let [builder (j/streams-builder)]
    (topology builder)
    (doto (j/kafka-streams builder config)
      (j/start))))


(comment
 ;; Test out Transducer
 (into []
       process-repayments-xf
       [[:k {:id 123
             :timestamp (System/currentTimeMillis)
             :payment-method :direct-debit
             :amount 199.99
             :user-id 5432}]])
 )