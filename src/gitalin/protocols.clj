(ns gitalin.protocols)

(defprotocol ICoatom
  (id [this])
  (property [this])
  (value [this]))

(defprotocol IAdapter
  (connect [this])
  (disconnect [this])
  (references->atoms [this])
  (reference->atoms [this id])
  (commits->atoms [this])
  (commit->atoms [this id])
  (transact! [this info mutations]))

(defprotocol IConnection
  (conn-id [this])
  (adapter [this]))
