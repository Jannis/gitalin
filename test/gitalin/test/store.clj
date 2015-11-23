(ns gitalin.test.store
  (:import [gitalin.core Connection])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [gitalin.test.setup :as setup :refer [with-conn]]
            [gitalin.core :as c]
            [gitalin.protocols :as p]
            [gitalin.adapter :as a]))

(defspec vector-satisfies-atom-interface
  (prop/for-all [v (gen/tuple (gen/not-empty gen/string)
                              gen/keyword
                              gen/any)]
    (let [atom (into [] v)]
      (and (is (satisfies? p/ICoatom atom))
           (is (= (p/id atom) (v 0)))
           (is (= (p/property atom) (v 1)))
           (is (= (p/value atom) (v 2)))))))

(defspec connection-is-constructed-correctly 10
  (prop/for-all [adapter setup/gen-store]
    (with-conn (c/connect adapter)
      (and (is (c/connection? conn))
           (is (satisfies? p/IConnection conn))))))

(defspec empty-store-has-no-atoms 5
  (prop/for-all [adapter setup/gen-store]
    (with-conn (c/connect adapter)
      (is (= [] (a/repo->atoms (p/adapter conn)))))))

(defn find-atoms [atoms v]
  (filter (fn [atom]
            (every? identity
                    (map-indexed #(or (nil? %2)
                                      (= (get atom %1) %2))
                                 v)))
          atoms))

(defn find-atom [atoms v]
  (first (find-atoms atoms v)))

(defspec adding-one-object-creates-atoms 5
  (prop/for-all [v (gen/tuple setup/gen-store
                              gen/uuid
                              gen/keyword
                              gen/any)]
    (let [[adapter uuid property string] v]
      (with-conn (c/connect adapter)
        (c/transact! conn
                     {:target "HEAD"
                      :author {:name "Test User" :email "<test@user.org>"}
                      :message "Create object"}
                     [[:object/add "class" (str uuid) property string]])
        (let [atoms (a/repo->atoms (p/adapter conn))
              head-commit (find-atom atoms ["HEAD" :ref/commit nil])
              master-commit (find-atom atoms
                                       ["refs:heads:master"
                                        :ref/commit])]
          (and (is (every? vector? atoms))
               (is (every? #(= 3 (count %)) atoms))
               (is (= 14 (count atoms)))

               ;; Verify ref atoms are present
               (is (some #{["HEAD" :ref/name "HEAD"]} atoms))
               (is (some #{["HEAD" :ref/type "branch"]} atoms))
               (is (some #{["refs:heads:master"
                            :ref/name
                            "refs:heads:master"]}
                         atoms))
               (is (some #{["refs:heads:master" :ref/type "branch"]}
                         atoms))
               (is (not (nil? head-commit)))
               (is (not (nil? master-commit)))
               (is (= (head-commit 2) (master-commit 2)))

               ;; Verify commit atoms are present
               (is (not (empty? (find-atoms atoms
                                            [nil :commit/sha1 nil]))))
               (is (not (empty? (find-atoms atoms
                                            [nil :commit/author]))))
               (is (not (empty? (find-atoms atoms
                                            [nil :commit/committer]))))
               (is (not (empty? (find-atoms atoms
                                            [nil
                                             :commit/message
                                             "Create object"]))))))))))

(defspec querying-refs-works 5
  (prop/for-all [v (gen/tuple setup/gen-store
                              gen/uuid
                              gen/keyword
                              gen/any)]
    (let [[adapter uuid property string] v]
      (with-conn (c/connect adapter)
        (c/transact! conn
                     {:target "HEAD"
                      :author {:name "Test User" :email "<test@user.org>"}
                      :message "Create object"}
                     [[:object/add "class" (str uuid) property string]])
        (is (= "HEAD"
               (c/q conn
                    '{:find ?ref
                      :where [[?ref :ref/name "HEAD"]]})))
        (is (= [["HEAD" "HEAD"]
                ["refs:heads:master" "refs:heads:master"]]
               (c/q conn
                    '{:find [?name ?ref]
                      :where [[?ref :ref/name ?name]]})))
        (is (= "HEAD"
                (c/q conn
                     '{:find ?ref
                       :where
                       [[?ref :ref/name ?name]
                        (some #{?name} ["HEAD"])]})))
        (is (= "branch"
               (c/q conn
                    '{:find ?type
                      :where [[?ref :ref/type ?type]]})))
        (is (= ["HEAD" "refs:heads:master"]
               (c/q conn
                    '{:find ?ref
                      :in ?type
                      :where [[?ref :ref/type ?type]]}
                    "branch")))))))
