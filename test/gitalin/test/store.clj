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
              head-commit (first (filter #(= ["HEAD" :ref/commit]
                                             [(s/id %) (s/property %)])
                                         atoms))
              master-commit (first (filter #(= ["refs:heads:master" :ref/commit]
                                               [(s/id %) (s/property %)])
                                           atoms))]
          (and (is (every? vector? atoms))
               (is (every? #(= 3 (count %)) atoms))
               (is (some #{["HEAD" :ref/name "HEAD"]} atoms))
               (is (some #{["HEAD" :ref/type "branch"]} atoms))
               (is (some #{["refs:heads:master"
                            :ref/name
                            "refs:heads:master"]}
                         atoms))
               (is (some #{["refs:heads:master"
                            :ref/type
                            "branch"]}
                         atoms))
               (is (not (nil? head-commit)))
               (is (not (nil? master-commit)))
               (is (= (head-commit 2) (master-commit 2)))))))))
