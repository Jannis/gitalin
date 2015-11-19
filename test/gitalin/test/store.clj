(ns gitalin.test.store
  (:import [gitalin.core Connection])
  (:require [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [gitalin.test.setup :as setup :refer [with-conn]]
            [gitalin.core :as c]
            [gitalin.store :as store]))

(defspec connection-is-constructed-correctly 10
  (prop/for-all [path setup/gen-store]
    (with-conn (c/connect path)
      (and (is (instance? Connection conn))
           (is (satisfies? c/IConnection conn))
           (is (= path (c/path conn)))))))

(defspec empty-store-has-no-atoms 5
  (prop/for-all [path setup/gen-store]
    (with-conn (c/connect path)
      (is (= [] (store/repo->atoms (:repo conn)))))))
