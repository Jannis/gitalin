(ns gitalin.git.coerce
  (:import [org.eclipse.jgit.lib FileMode])
  (:require [clojure.string :as str]))

(defn to-sha1 [oid]
  (.getName oid))

(defn to-oid [repo sha1]
  (.resolve (.getRepository repo) sha1))

(defn to-git-ref-name [name]
  (str/replace name #":" "/"))

(defn to-ref-name [name]
  (str/replace name #"/" ":"))

(defn to-file-mode [mode]
  (case mode
    :tree FileMode/TREE
    :file FileMode/REGULAR_FILE
          nil))
