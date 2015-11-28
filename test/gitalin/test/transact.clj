(ns gitalin.test.transact
  (:import [gitalin.core Connection])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [gitalin.test.setup :as setup :refer [with-conn]]
            [gitalin.core :as c]
            [gitalin.query :as q]
            [gitalin.protocols :as p]
            [gitalin.adapter :as a]))

(defspec as-many-commits-created-as-transactions 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-add-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (let [res (c/q conn '{:find ?c
                            :where [[?c :commit/sha1 ?s]]})]
        (is (= (count transactions)
               (count res)))))))

(defspec each-commit-correspond-to-one-transaction 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-add-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (is (= (into #{} (map #(:message (:info %)) transactions))
             (c/q conn '{:find ?msg
                         :where [[?c :commit/message ?msg]]}))))))

(defspec transactions-update-HEAD 10
  (prop/for-all [store setup/gen-store
                 txs setup/gen-add-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (every? true?
       (for [{:keys [info data]} txs]
         (do
           (c/transact! conn info data)
           (is (= #{(:message info)}
                  (c/q conn '{:find ?msg
                              :where
                              [[?ref :ref/name "HEAD"]
                               [?ref :ref/commit ?commit]
                               [?commit :commit/message ?msg]]})))))))))

(defspec transactions-update-master 10
  (prop/for-all [store setup/gen-store
                 txs setup/gen-add-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (every? true?
       (for [{:keys [info data]} txs]
         (do
           (c/transact! conn info data)
           (is (= #{(:message info)}
                  (c/q conn '{:find ?msg
                              :where
                              [[?ref :ref/name "refs:heads:master"]
                               [?ref :ref/commit ?commit]
                               [?commit :commit/message ?msg]]})))))))))

(defspec HEAD-and-master-point-to-the-same-commit 10
  (prop/for-all [store setup/gen-store
                 txs setup/gen-add-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (every? true?
       (for [{:keys [info data]} txs]
         (do
           (c/transact! conn info data)
           (is (= (c/q conn '{:find ?h
                              :where
                              [[?ref :ref/name "HEAD"]
                               [?ref :ref/commit ?h]]})
                  (c/q conn '{:find ?m
                              :where
                              [[?ref :ref/name "refs:heads:master"]
                               [?ref :ref/commit ?m]]})))))))))

(defn object-add? [mutation]
  (and (sequential? mutation)
       (= :object/add (first mutation))))

(defn get-object-additions [txs]
  (->> txs
       (map :data)
       (map #(filter object-add? %))
       (apply concat)))

(defspec each-created-object-exists-in-the-corresponding-transaction 10
  (prop/for-all [store setup/gen-store
                 txs setup/gen-add-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} txs]
        (c/transact! conn info data))
      (let [additions (get-object-additions txs) 
            data (map (fn [[_ uuid prop val]]
                        [uuid prop val])
                      additions)
            uuids-and-values (map (fn [[uuid _ val]]
                                    [uuid val])
                                  data)
            result (apply concat
                          (for [d data]
                            (let [[_ prop _] d]
                              (c/q conn
                                   `{:find [?u ?v]
                                     :where [[?ref :ref/name "HEAD"]
                                             [?ref :ref/commit ?commit]
                                             [?commit :commit/object ?o]
                                             [?o :object/uuid ?u]
                                             [?o ~prop ?v]]}))))]
        (is (= (into #{} uuids-and-values)
               (into #{} result)))))))

(defspec all-created-objects-are-still-present-at-the-end 10
  (prop/for-all [store setup/gen-store
                 txs setup/gen-add-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} txs]
        (c/transact! conn info data))
      (let [uuids (->> (get-object-additions txs)
                       (map (fn [[_ uuid _]] uuid)))]
        (is (= (into #{} uuids)
               (into #{} (c/q conn
                              '{:find ?u
                                :where [[?ref :ref/name "HEAD"]
                                        [?ref :ref/commit ?commit]
                                        [?commit :commit/object ?o]
                                        [?o :object/uuid ?u]]}))))))))

(defspec temporary-ids-are-translated-to-real-ones 10
  (prop/for-all [store setup/gen-store
                 txs setup/gen-tempid-add-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} txs]
        (c/transact! conn info data))
      (is (not-any? #(a/tempid? %)
                    (c/q conn '{:find ?u
                                :where [[?o :object/uuid ?u]]}))))))
