(ns gitalin.test.query
  (:import [gitalin.core Connection])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [gitalin.test.setup :as setup :refer [with-conn]]
            [gitalin.test.transact :refer [object-add?]]
            [gitalin.core :as c]
            [gitalin.query :as q]
            [gitalin.protocols :as p]
            [gitalin.adapter :as a]))

;;;; Reference queries

(defspec querying-refs-after-empty-transactions-returns-nothing 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (not (empty? transactions))
          (and (is (= #{}
                      (c/q conn '{:find ?n
                                  :where [[?ref :ref/name ?n]]})))
               (is (= #{}
                      (c/q conn '{:find ?ref
                                  :where [[?ref :ref/name "HEAD"]]})))
               (is (= #{}
                      (c/q conn '{:find ?t
                                  :where [[?ref :ref/type ?t]]})))
               (is (= #{}
                      (c/q conn '{:find ?c
                                  :where [[?ref :ref/commit ?c]]}))))))))

(defspec ref-names-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (is (= #{"HEAD" "refs:heads:master"}
                 (c/q conn '{:find ?n
                             :where [[?ref :ref/name ?n]]})))))))

(defspec ref-ids-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (is (= #{"reference/HEAD" "reference/refs:heads:master"}
                 (c/q conn '{:find ?ref
                             :where [[?ref :ref/name ?n]]})))))))

(defspec ref-types-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (is (= #{"branch"}
                 (c/q conn '{:find ?t
                             :where [[?ref :ref/type ?t]]})))))))

(defspec ref-commits-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (and
           (is (= 1
                  (count
                   (c/q conn '{:find ?c
                               :where [[?ref :ref/commit ?c]]}))))
           (is (re-matches
                #"commit/[0-9abcdef]{40}"
                (first
                 (c/q conn '{:find ?c
                             :where [[?ref :ref/commit ?c]]})))))))))

(defspec multiple-refs-properties-can-be-queried-at-once 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (is (= #{["HEAD"
                    "reference/HEAD"]
                   ["refs:heads:master"
                    "reference/refs:heads:master"]}
                 (c/q conn '{:find [?name ?ref]
                             :where [[?ref :ref/name ?name]]})))))))

(defspec ref-names-can-be-parameterized 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
                (with-conn (assoc (c/connect store) :debug false)
                  (doseq [{:keys [info data]} transactions]
                    (c/transact! conn info data))
                  (or (empty? transactions)
                      (is (= #{"reference/HEAD"}
                             (c/q conn '{:find ?ref
                                         :in ?name
                                         :where [[?ref :ref/name ?name]]}
                                  "HEAD")))))))

(defspec ref-ids-can-be-parameterized 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (is (= #{"HEAD"}
                 (c/q conn '{:find ?name
                             :in ?ref
                             :where [[?ref :ref/name ?name]]}
                      "reference/HEAD")))))))

(defspec ref-types-can-be-parameterized 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (is (= #{"reference/HEAD" "reference/refs:heads:master"}
                 (c/q conn '{:find ?ref
                             :in ?type
                             :where [[?ref :ref/type ?type]]}
                      "branch")))))))

;;;; Commit queries

(defspec querying-commits-after-empty-transactions-returns-nothing 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (not (empty? transactions))
          (and (is (= #{}
                      (c/q conn
                           '{:find ?s
                             :where [[?commit :commit/sha1 ?s]]})))
               (is (= #{}
                      (c/q conn
                           '{:find ?a
                             :where [[?commit :commit/author ?a]]})))
               (is (= #{}
                      (c/q conn
                           '{:find ?c
                             :where [[?commit :commit/committer ?c]]})))
               (is (= #{}
                      (c/q conn
                           '{:find ?m
                             :where [[?commit :commit/message ?m]]})))
               (is (= #{}
                      (c/q conn
                           '{:find ?c
                             :where [[?commit :commit/class ?c]]})))
               (is (= #{}
                      (c/q conn
                           '{:find ?p
                             :where [[?commit :commit/parent ?p]]}))))))))

(defspec commit-sha1s-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [sha1s (c/q conn '{:find ?s
                                  :where [[?c :commit/sha1 ?s]]})]
            (and (is (set? sha1s))
                 (is (every? #(re-matches #"[0-9abcdef]{40}" %)
                             sha1s))))))))

(defspec commit-authors-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [expected-authors (into #{}
                                       (comp (map :info)
                                             (map :author))
                                       transactions)
                authors (c/q conn '{:find ?a
                                    :where [[?c :commit/author ?a]]})
                authors (into #{}
                              (map #(select-keys % [:name :email]))
                              authors)]
            (is (= expected-authors authors)))))))

(defspec commit-committers-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [expected-committers (into #{}
                                          (comp (map :info)
                                                (map :committer))
                                          transactions)
                committers (c/q conn
                                '{:find ?cm
                                  :where [[?c :commit/committer ?cm]]})
                committers (into #{}
                              (map #(select-keys % [:name :email]))
                              committers)]
            (is (= expected-committers committers)))))))

(defspec commit-messages-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [expected-messages (into #{}
                                        (comp (map :info)
                                              (map :message))
                                          transactions)
                messages (c/q conn
                              '{:find ?msg
                                :where [[?c :commit/message ?msg]]})]
            (and (is (set? messages))
                 (is (= expected-messages messages))))))))

(defspec commit-parents-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [parents (c/q conn
                             '{:find ?p
                               :where [[?c :commit/parent ?p]]})]
            (and (is (= (- (count transactions) 1)
                        (count parents)))
                 (is (every? #(re-matches #"commit/[0-9abcdef]{40}" %)
                             parents))))))))

(defspec commit-parents-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [expected-classes (->> transactions
                                      (map :data)
                                      (map (fn [t] (map second t)))
                                      (apply concat)
                                      (into #{}))
                classes (c/q conn
                             '{:find ?cln
                               :where [[?c :commit/class ?cl]
                                       [?cl :class/name ?cln]]})]
            (is (= expected-classes classes)))))))

;;;; Class queries

(defspec querying-classes-after-empty-transactions-returns-nothing 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (not (empty? transactions))
          (and (is (= #{}
                      (c/q conn
                           '{:find ?s
                             :where [[?class :class/name ?s]]})))
               (is (= #{}
                      (c/q conn
                           '{:find ?c
                             :where [[?class :class/commit ?c]]})))
               (is (= #{}
                      (c/q conn
                           '{:find ?o
                             :where [[?class :class/object ?o]]}))))))))

(defspec class-names-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [expected-names (->> transactions
                                    (map :data)
                                    (map (fn [t] (map second t)))
                                    (apply concat)
                                    (into #{}))
                names (c/q conn '{:find ?n
                                  :where [[?c :class/name ?n]]})]
            (is (= expected-names names)))))))

(defspec class-commits-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug true)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [non-empty-transactions (->>
                                        transactions
                                        (map :data)
                                        (map #(filter object-add? %))
                                        (remove empty?))
                commits (c/q conn '{:find ?c
                                    :where [[?class :class/commit ?c]]})]
            (and (is (set? commits))
                 (is (every? #(re-matches #"commit/[0-9abcdef]{40}" %)
                             commits))))))))

(defspec class-objects-can-be-queried 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (or (empty? transactions)
          (let [objects (c/q conn '{:find ?o
                                    :where [[?class :class/object ?o]]})]
            (and (is (set? objects))
                 (is (every? #(re-matches #"object/.+" %)
                             objects))))))))

;; TODO:
;; * Tests for querying objects
