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

;; (defspec empty-store-has-no-atoms
;;   (prop/for-all [adapter setup/gen-store]
;;     (with-conn (c/connect adapter)
;;       (is (= [] (a/repo->atoms (p/adapter conn)))))))

;; (defn find-atoms [atoms v]
;;   (filter (fn [atom]
;;             (every? identity
;;                     (map-indexed #(or (nil? %2)
;;                                       (= (get atom %1) %2))
;;                                  v)))
;;           atoms))

;; (defn find-atom [atoms v]
;;   (first (find-atoms atoms v)))

;; (defspec adding-one-object-creates-atoms
;;   (prop/for-all [v (gen/tuple setup/gen-store
;;                               gen/uuid
;;                               gen/keyword
;;                               gen/any)]
;;     (let [[adapter uuid property string] v]
;;       (with-conn (c/connect adapter)
;;         (c/transact! conn
;;                      {:target "HEAD"
;;                       :author {:name "Test User" :email "<test@user.org>"}
;;                       :message "Create object"}
;;                      [[:object/add "class" (str uuid) property string]])
;;         (let [atoms (a/repo->atoms (p/adapter conn))
;;               head-commit (find-atom atoms ["reference/HEAD"
;;                                             :ref/commit
;;                                             nil])
;;               master-commit (find-atom atoms
;;                                        ["reference/refs:heads:master"
;;                                         :ref/commit])]
;;           (and (is (every? vector? atoms))
;;                (is (every? #(= 3 (count %)) atoms))
;;                (is (= 14 (count atoms)))

               ;; ;; Verify ref atoms are present
               ;; (is (some #{["reference/HEAD" :ref/name "HEAD"]} atoms))
               ;; (is (some #{["reference/HEAD" :ref/type "branch"]} atoms))
               ;; (is (some #{["reference/refs:heads:master"
               ;;              :ref/name
               ;;              "refs:heads:master"]}
               ;;           atoms))
               ;; (is (some #{["reference/refs:heads:master"
               ;;              :ref/type
               ;;              "branch"]}
               ;;           atoms))
               ;; (is (not (nil? head-commit)))
               ;; (is (not (nil? master-commit)))
               ;; (is (= (head-commit 2) (master-commit 2)))

               ;; ;; Verify commit atoms are present
               ;; (is (not (empty? (find-atoms atoms
               ;;                              [nil :commit/sha1 nil]))))
               ;; (is (not (empty? (find-atoms atoms
               ;;                              [nil :commit/author]))))
               ;; (is (not (empty? (find-atoms atoms
               ;;                              [nil :commit/committer]))))
               ;; (is (not (empty? (find-atoms atoms
               ;;                              [nil
               ;;                               :commit/message
               ;;                               "Create object"]))))))))))

;; (defspec querying-refs-works
;;   (prop/for-all [v (gen/tuple setup/gen-store
;;                               gen/uuid
;;                               gen/keyword
;;                               gen/any)]
;;     (let [[adapter uuid property string] v]
;;       (with-conn (c/connect adapter)
;;         (c/transact! conn
;;                      {:target "HEAD"
;;                       :author {:name "Test User"
;;                                :email "<test@user.org>"}
;;                       :message "Create object"}
;;                      [[:object/add "class" (str uuid) property string]])

;;         ;; Verify there is a HEAD ref
;;         (is (= "reference/HEAD"
;;                (c/q conn
;;                     '{:find ?ref
;;                       :where [[?ref :ref/name "HEAD"]]})))

;;         ;; Verify we can query for ref names
;;         (is (= "HEAD"
;;                (c/q conn
;;                     '{:find ?name
;;                       :where [["reference/HEAD" :ref/name ?name]]})))

;;         ;; Verify we can query for ref types
;;         (is (= "branch"
;;                (c/q conn
;;                     '{:find ?type
;;                       :where [[?ref :ref/type ?type]]})))

;;         ;; Verify we can query for ref commits
;;         (is (re-matches #"commit/[0-9abcdef]{40}"
;;                         (c/q conn
;;                              '{:find ?sha1
;;                                :where [[?ref :ref/name "HEAD"]
;;                                        [?ref :ref/commit ?sha1]]})))

;;         ;; Verify we can query for multiple ref props at the same time
;;         (is (= #{["HEAD" "reference/HEAD"]
;;                  ["refs:heads:master" "reference/refs:heads:master"]}
;;                (c/q conn
;;                     '{:find [?name ?ref]
;;                       :where [[?ref :ref/name ?name]]})))

;;         ;; Verify (some ...) works to filter results
;;         (is (= "reference/HEAD"
;;                 (c/q conn
;;                      '{:find ?ref
;;                        :where
;;                        [[?ref :ref/name ?name]
;;                         (some #{?name} ["HEAD"])]})))

;;         ;; Verify passing a prop value in works
;;         (is (= #{"reference/HEAD" "reference/refs:heads:master"}
;;                (c/q conn
;;                     '{:find ?ref
;;                       :in ?type
;;                       :where [[?ref :ref/type ?type]]}
;;                     "branch")))))))

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

(defspec as-many-commits-created-as-transactions 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (c/connect store)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (let [res (c/q conn '{:find ?c
                            :where [[?c :commit/sha1 ?s]]})]
        (is (= (count transactions)
               (count res)))))))

(defspec each-commit-correspond-to-one-transaction 10
  (prop/for-all [store setup/gen-store
                 transactions setup/gen-transactions]
    (with-conn (c/connect store)
      (doseq [{:keys [info data]} transactions]
        (c/transact! conn info data))
      (is (= (into #{} (map #(:message (:info %)) transactions))
             (c/q conn '{:find ?msg
                         :where [[?c :commit/message ?msg]]}))))))

(defspec transactions-update-HEAD 10
  (prop/for-all [store setup/gen-store
                 txs setup/gen-transactions]
    (with-conn (c/connect store)
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
                 txs setup/gen-transactions]
    (with-conn (c/connect store)
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

(defspec HEAD-and-master-point-to-the-same-commit 5
  (prop/for-all [store setup/gen-store
                 txs setup/gen-transactions]
    (with-conn (c/connect store)
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

(defspec adding-objects-creates-classes 5
  (prop/for-all [store setup/gen-store
                 txs setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug false)
      (doseq [{:keys [info data]} txs]
        (c/transact! conn info data))
      (let [adds (->> txs
                      (map :data)
                      (map #(filter object-add? %))
                      (apply concat))
            class-names (map second adds)]
         (is (= (set class-names)
                (c/q conn '{:find ?name
                            :where
                            [[?ref :ref/name "HEAD"]
                             [?ref :ref/commit ?c]
                             [?c :commit/class ?class]
                             [?class :class/name ?name]]})))))))

(defn var-from-index [i]
  (symbol (str "?v" i)))

(defspec adding-objects-actually-creates-these-objects 5
  (prop/for-all [store setup/gen-store
                 txs setup/gen-transactions]
    (with-conn (assoc (c/connect store) :debug true)
      (doseq [{:keys [info data]} txs]
        (c/transact! conn info data))
      (let [additions (->> txs
                           (map :data)
                           (map #(filter object-add? %))
                           (apply concat))
            data (map (fn [[_ class uuid prop val]]
                        [class uuid prop val])
                      additions)]
        (is (= (set (map (fn [[class uuid _ val]]
                           [class uuid val])
                         data))
               (into #{}
                (apply concat
                 (for [d data]
                   (let [[class uuid prop val] d]
                     (c/q conn `{:find [?c ?u ?v]
                                 :where [[?ref :ref/name "HEAD"]
                                         [?ref :ref/commit ?commit]
                                         [?commit :commit/class ?class]
                                         [?class :class/object ?o]
                                         [?o :object/class ?c]
                                         [?o :object/uuid ?u]
                                         [?o ~prop ?v]]})))))))))))
