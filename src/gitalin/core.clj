(ns gitalin.core
  (:require [gitalin.git.reference :as reference]
            [gitalin.git.repo :as git-repo]
            [gitalin.protocols :as p]
            [gitalin.query :as query]))

;;;; Connections

(defrecord Connection [id adapter]
  p/IConnection
  (conn-id [this]
    id)
  (adapter [this]
    (:adapter this)))

(defn adapter [conn]
  (p/adapter conn))

(defn connect [adapter]
  (Connection. (java.util.UUID/randomUUID) (p/connect adapter)))

;;;; Misc

(defn create-store! [path]
  (if (git-repo/init path) path nil))

(defn connection? [conn]
  (instance? Connection conn))

(defn commit-info? [info]
  (map? info))

(defn transact! [conn info mutations]
  {:pre [(satisfies? p/IConnection conn)
         (commit-info? info)
         (vector? mutations)]}
  (p/transact! (p/adapter conn) info mutations))

(defn q [conn q & args]
  {:pre [(satisfies? p/IConnection conn)
         (map? q)]}
  (query/q conn q args))
