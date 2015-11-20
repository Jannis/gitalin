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

(defn transact! [conn info mutations]
  {:pre [(connection? conn)]}
  (store/transact! (:repo conn) info mutations))
