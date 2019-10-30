(ns ks-talk.05-complex-willa
  (:require [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse]
            [willa.core :as w]
            [willa.specs :as ws]
            [willa.viz :as wv]
            [willa.experiment :as we]
            [clojure.spec.alpha :as s]))


(def config
  {"application.id" "repayments-processor-jackdaw"
   "bootstrap.servers" "localhost:9092"})

(defn ->topic-config [topic-name]
  {:topic-name topic-name
   :partition-count 1
   :replication-factor 1
   :topic-config {}
   :key-serde (jse/serde)
   :value-serde (jse/serde)
   ::w/entity-type :topic})

(def process-incoming-transaction-xf
  (map (fn [[k v]]
         [k (-> (select-keys v [:id :timestamp :payment-method])
                (assoc :amount-cents (int (* (:amount v) 100))))])))

(def process-repayments-xf
  (comp
   process-incoming-transaction-xf
   (filter (fn [[k v]]
             (= (:payment-method v) :direct-debit)))))

(def process-reversals-xf
  (comp
   process-incoming-transaction-xf
   (map (fn [[k v]]
          [k (update v :amount-cents -)]))))

(def entities
  {:topic/repayments (->topic-config "repayments")
   :topic/reversals (->topic-config "reversals")
   :topic/direct-debit-transactions (->topic-config "direct-debit-transactions")

   :stream/repayments {::w/entity-type :kstream
                       ::w/xform process-repayments-xf}
   :stream/reversals {::w/entity-type :kstream
                      ::w/xform process-reversals-xf}})

(def workflow
  [[:topic/repayments :stream/repayments]
   [:topic/reversals :stream/reversals]
   [:stream/repayments :topic/direct-debit-transactions]
   [:stream/reversals :topic/direct-debit-transactions]])

(def joins
  {[:stream/repayments :stream/reversals] {::w/join-type :merge}})

(def topology
  {:entities entities
   :workflow workflow
   :joins joins})

(defn start! []
  (let [builder (j/streams-builder)]
    (w/build-topology! builder topology)
    (doto (j/kafka-streams builder config)
      (j/start))))


(comment
 ;; Test out Transducers
 (into []
       process-repayments-xf
       [[:k {:id 123
             :timestamp (System/currentTimeMillis)
             :payment-method :direct-debit
             :amount 199.99
             :user-id 5432}]])

 (into []
       process-reversals-xf
       [[:k {:id 234
             :payment-reversed-id 123
             :timestamp (System/currentTimeMillis)
             :amount 199.99
             :user-id 5432}]])


 ;; Validate topology
 (s/explain :willa.specs/topology topology)


 ;; Visualise topology
 (willa.viz/view-topology topology)

 ;; Experiment
 (def experiment-output
   (willa.experiment/run-experiment
    topology
    {:topic/repayments [{:key :k
                         :value {:id 123
                                 :timestamp (System/currentTimeMillis)
                                 :payment-method :direct-debit
                                 :amount 199.99
                                 :user-id 5432}
                         :timestamp 0}]}))

 (willa.experiment/results-only experiment-output)

 (willa.viz/view-topology experiment-output)
 )