(ns gitalin.test.coatom
  (:require [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [gitalin.coatom :as coatom]))

(defspec coatom-constructor-sets-properties
  (prop/for-all [v (gen/tuple gen/string gen/keyword gen/any)]
    (let [atom (coatom/->CoAtom (v 0) (v 1) (v 2))]
      (is (= (select-keys atom [:id :property :value])
             {:id (v 0) :property (v 1) :value (v 2)})))))

(defspec coatom-satisfies-atom-interface
  (prop/for-all [v (gen/tuple gen/string gen/keyword gen/any)]
    (let [atom (coatom/->CoAtom (v 0) (v 1) (v 2))]
      (and (is (satisfies? coatom/ICoAtom atom))
           (is (= (coatom/id atom) (v 0)))
           (is (= (coatom/property atom) (v 1)))
           (is (= (coatom/value atom) (v 2)))))))
