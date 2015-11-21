(ns gitalin.test.store
  (:import [gitalin.core Connection])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [gitalin.test.setup :as setup :refer [with-conn]]
            [gitalin.core :as c]
            [gitalin.store :as s]))

(defspec vector-satisfies-atom-interface
  (prop/for-all [v (gen/tuple (gen/not-empty gen/string)
                              gen/keyword
                              gen/any)]
    (let [atom (into [] v)]
      (and (is (satisfies? s/ICoAtom atom))
           (is (= (s/id atom) (v 0)))
           (is (= (s/property atom) (v 1)))
           (is (= (s/value atom) (v 2)))))))

(defspec connection-is-constructed-correctly 10
  (prop/for-all [path setup/gen-store]
    (with-conn (c/connect path)
      (and (is (c/connection? conn))
           (is (satisfies? c/IConnection conn))
           (is (= path (c/path conn)))))))

(defspec empty-store-has-no-atoms 5
  (prop/for-all [path setup/gen-store]
    (with-conn (c/connect path)
      (is (= [] (s/repo->atoms (:repo conn)))))))

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
    (let [[path uuid property string] v]
      (with-conn (c/connect path)
        (c/transact! conn
                     {:target "HEAD"
                      :author {:name "Test User" :email "<test@user.org>"}
                      :message "Create object"}
                     [[:store/add "class" (str uuid) property string]])
        (let [atoms (s/repo->atoms (:repo conn))
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

(defspec simple-query-for-head-ref
  (prop/for-all [v (gen/tuple setup/gen-store
                              gen/uuid
                              gen/keyword
                              gen/any)]
    (let [[path uuid property string] v]
      (with-conn (c/connect path)
        (c/transact! conn
                     {:target "HEAD"
                      :author {:name "Test User" :email "<test@user.org>"}
                      :message "Create object"}
                     [[:store/add "class" (str uuid) property string]])
        (is (= "HEAD"
               (c/q conn
                    '{:find ?ref
                      :where [[?ref :ref/name "HEAD"]]})))
        (is (= ["refs:heads:master" "refs:heads:master"]
               (c/q conn
                    '{:find [?name ?ref]
                      :where [[?ref :ref/name ?name]]})))
        (is (= ["HEAD" "refs:heads:master"]
                (c/q conn
                     '{:find [?head ?master]
                       :where
                       [[?head :ref/name "HEAD"]
                        [?master :ref/name "refs:heads:master"]]})))
        (is (= "branch"
               (c/q conn
                    '{:find ?type
                      :where [[?ref :ref/type ?type]]})))))))
