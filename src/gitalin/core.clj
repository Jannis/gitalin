(ns gitalin.core
  (:require [gitalin.git.reference :as reference]
            [gitalin.git.repo :as git-repo]
            [gitalin.adapter :as a]
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

(defn connection? [conn]
  (instance? Connection conn))

(defn default-adapter [path]
  (a/adapter path))

;;;; Store creation

(defn create-store! [path]
  (if (git-repo/init path) path nil))

;;;; Queries

(defn q [conn q & args]
  {:pre [(satisfies? p/IConnection conn)
         (map? q)]}
  (query/q conn q args))

;;;; Transactions

(defn transact! [conn info mutations]
  {:pre [(satisfies? p/IConnection conn)
         (map? info)
         (vector? mutations)]}
  (p/transact! (p/adapter conn) info mutations))
