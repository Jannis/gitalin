(ns gitalin.coatom)

(defprotocol ICoAtom
  (id [this])
  (property [this])
  (value [this]))

(defrecord CoAtom [id property value]
    ICoAtom
    (id [this]
      (:id this))
    (property [this]
      (:property this))
    (value [this]
      (:value this)))
