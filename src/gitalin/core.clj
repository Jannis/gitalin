(ns gitalin.core
  (:require [gitalin.git.repo :as git-repo]
            [gitalin.store :as store]))

(defprotocol IConnection
  (path [this]))

(defrecord Connection [path repo]
  IConnection
  (path [this]
    (:path this)))

(defn create-store! [path]
  (if (git-repo/init path) path nil))

(defn connect [path]
  (->Connection path (git-repo/load path)))

(defn connection? [conn]
  (instance? Connection conn))

(defn commit-info? [info]
  (map? info))

(defn transact! [conn info mutations]
  {:pre [(connection? conn) (commit-info? info) (vector? mutations)]}
  (store/transact! (:repo conn) info mutations))

(defn q [conn q & args]
  {:pre [(map? q)]}
  (store/q (:repo conn) q args))
