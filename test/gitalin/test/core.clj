(ns gitalin.test.core
  (:require [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [gitalin.core :as c]
            [gitalin.protocols :as p]
            [gitalin.test.setup :as setup :refer [with-conn]]))

(defspec connection-is-constructed-correctly
  (prop/for-all [adapter setup/gen-store]
    (with-conn (c/connect adapter)
      (and (c/connection? conn)
           (satisfies? p/IConnection conn)
           (satisfies? p/IAdapter (p/adapter conn))))))
