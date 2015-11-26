(ns gitalin.test.query
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
    (with-conn (assoc (c/connect store) :debug true)
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

;; (defspec querying-commits-works
;;   (prop/for-all [v (gen/tuple setup/gen-store
;;                               gen/uuid
;;                               gen/uuid
;;                               gen/keyword
;;                               gen/keyword
;;                               gen/any
;;                               gen/any)]
;;     (let [[adapter uuid1 uuid2 prop1 prop2 val1 val2] v]
;;       (with-conn (c/connect adapter)
;;         (c/transact! conn
;;                      {:target "HEAD"
;;                       :author {:name "Test User"
;;                                :email "<test@user.org>"}
;;                       :message "Create object"}
;;                      [[:object/add "class" (str uuid1) prop1 val1]])
;;         (c/transact! conn
;;                      {:target "HEAD"
;;                       :author {:name "Other User"
;;                                :email "<other@user.org>"}
;;                       :message "Create another object"}
;;                      [[:object/add "other" (str uuid2) prop2 val2]])

;;         ;; Verify there are two separate commits
;;         (let [res (c/q conn
;;                        '{:find ?c
;;                          :where [[?c :commit/sha1 ?s]]})]
;;           (and (is (set? res))
;;                (is (= 2 (count res)))
;;                (is (every? string? res))
;;                (is (every? #(re-matches
;;                              #"commit/[0-9abcdef]{40}"
;;                              %)
;;                            res))))

;;         ;; Verify that there are two different commit messages
;;         (is (= #{"Create object" "Create another object"}
;;                (c/q conn
;;                     '{:find ?m
;;                       :where [[?c :commit/message ?m]]})))

;;         ;; Verify that HEAD points to the second transaction commit
;;         (let [res (c/q conn
;;                        '{:find ?msg
;;                          :where [[?ref :ref/name "HEAD"]
;;                                  [?ref :ref/commit ?commit]
;;                                  [?commit :commit/message ?msg]]})]
;;           (is (= "Create another object" res)))))))
