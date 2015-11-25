(ns gitalin.protocols
  (:refer-clojure :exclude [class]))

(defprotocol IEntity
  (id [this])
  (properties [this]))

(defprotocol IAdapter
  (connect [this])
  (disconnect [this])
  (references [this])
  (reference [this id])
  (commits [this])
  (commit [this id])
  (classes [this])
  (class [this id])
  (objects [this])
  (object [this id])
  (transact! [this info mutations]))

(defprotocol IConnection
  (conn-id [this])
  (adapter [this]))
