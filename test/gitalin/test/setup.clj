(ns gitalin.test.setup
  (:require [clojure.java.io :as io]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [gitalin.git.repo :as repo]
            [gitalin.adapter :as a]
            [gitalin.core :as c]))

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
    (a/adapter (c/create-store! path))))

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
       ~@body
       (delete-store-from-conn ~(symbol "conn"))
       (catch Exception e#
         (delete-store-from-conn ~(symbol "conn"))
         (throw e#)))))

;;;; Generate transactions

(def gen-transactions
  (gen/vector
   (gen/hash-map
    :info (gen/hash-map
           :target (gen/return "HEAD")
           :author (gen/hash-map
                    :name gen/string-alphanumeric
                    :email gen/string-alphanumeric)
           :committer (gen/hash-map
                       :name gen/string-alphanumeric
                       :email gen/string-alphanumeric)
           :message gen/string)
    :data (gen/vector
           (gen/fmap
            vec
            (gen/tuple (gen/return :object/add)
                       (gen/not-empty gen/string-alphanumeric)
                       (gen/fmap str gen/uuid)
                       gen/keyword
                       gen/any))))))
