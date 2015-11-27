(ns gitalin.test.setup
  (:require [clojure.java.io :as io]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [me.raynes.fs :as fs]
            [gitalin.git.repo :as repo]
            [gitalin.adapter :as a]
            [gitalin.core :as c]))

;;;; Misc helpers

(defmacro dofor
  [bindings & body]
  `(doall
    (for ~bindings
      ~@body)))

;;;; Create temporary directories

(def gen-temp-dir
  (gen/fmap (fn [some-int] (fs/temp-dir "gitalin")) gen/int))

(defspec temp-dir-generator-works 10
  (prop/for-all [dir gen-temp-dir]
    (try
      (fs/directory? dir)
      (finally (fs/delete-dir dir)))))

;;;; Create temporary stores

(defn init-store [dir]
  (let [path (.getAbsolutePath dir)]
    (c/default-adapter (c/create-store! path))))

(def gen-store
  (gen/fmap init-store gen-temp-dir))

(defn create-store []
  (-> (fs/temp-dir "gitalin")
      init-store))

(defn delete-store [path]
  (fs/delete-dir (io/as-file path)))

(defn create-store-with-conn []
  (-> (fs/temp-dir "gitalin")
      init-store
      c/connect))

(defn delete-store-from-conn [conn]
  (delete-store (:path (c/adapter conn))))

;;;; Use connections in specs

(defmacro with-conn
  [conn & body]
  `(let [~(symbol "conn") ~conn]
     (try
       (do
         ~@body)
       (finally
         (delete-store-from-conn ~(symbol "conn"))))))

;;;; Generate transactions

(def gen-transaction-info-HEAD
  (gen/hash-map
   :target (gen/return "HEAD")
   :author (gen/hash-map :name gen/string-alphanumeric
                         :email gen/string-alphanumeric)
   :committer (gen/hash-map :name gen/string-alphanumeric
                            :email gen/string-alphanumeric)
   :message gen/string))

(def gen-add-mutations
  (gen/vector-distinct
   (gen/fmap vec
             (gen/tuple (gen/return :object/add)
                        (gen/fmap str gen/uuid)
                        gen/keyword
                        gen/any))))

(def gen-add-transactions
  (gen/vector-distinct
   (gen/hash-map :info gen-transaction-info-HEAD
                 :data gen-add-mutations)))

(def gen-tempid
  (gen/fmap (fn [_] (a/tempid)) gen/int))

(def gen-add-tempid-mutations
  (gen/vector-distinct
   (gen/fmap vec
             (gen/tuple (gen/return :object/add)
                        gen-tempid
                        gen/keyword
                        gen/any))))

(def gen-tempid-add-transactions
  (gen/vector-distinct
   (gen/hash-map :info gen-transaction-info-HEAD
                 :data gen-add-tempid-mutations)))

(defn gen-add-mutation-for-uuid [uuid]
  (gen/fmap vec
            (gen/tuple (gen/return :object/add)
                       (gen/return uuid)
                       gen/keyword
                       gen/any)))

(defn gen-set-mutation-for-uuid [uuid]
  (gen/fmap vec
            (gen/tuple (gen/return :object/set)
                       (gen/return uuid)
                       gen/keyword
                       gen/any)))

(defn gen-set-mutations-for-uuid [uuid]
  (gen/vector
   (gen/bind (gen/return uuid)
             gen-set-mutation-for-uuid)))

(defn gen-add-set-mutations-for-uuid [uuid]
  (gen/fmap (fn [[add sets]]
              (into [] (concat [add] sets)))
            (gen/tuple
             (gen/bind (gen/return uuid)
                       gen-add-mutation-for-uuid)
             (gen/bind (gen/return uuid)
                       gen-set-mutations-for-uuid))))

(def gen-add-set-mutations
  (gen/fmap (fn [mutation-groups]
              (into [] (apply concat mutation-groups)))
            (gen/vector-distinct
             (gen/bind gen-tempid
                       gen-add-set-mutations-for-uuid))))

(def gen-add-set-transactions
  (gen/vector-distinct
   (gen/hash-map :info gen-transaction-info-HEAD
                 :data gen-add-set-mutations)))
