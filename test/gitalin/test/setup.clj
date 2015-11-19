(ns gitalin.test.setup
  (:require [clojure.java.io :as io]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.stuartsierra.component :as component]
            [me.raynes.fs :as fs]
            [gitalin.git.repo :as repo]
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
    (c/create-store! path)))

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
  (delete-store (c/path conn)))

;;;; Use connections in specs

(defmacro with-conn
  [conn & body]
  `(let [~(symbol "conn") ~conn]
     (try
       ~@body
       (finally
         (delete-store-from-conn ~(symbol "conn"))))))
