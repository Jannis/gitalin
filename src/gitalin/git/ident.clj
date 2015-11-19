(ns gitalin.git.ident
  (:import [java.util Calendar Date]
           [org.eclipse.jgit.lib PersonIdent])
  (:refer-clojure :exclude [load]))

(defrecord Identity [name email date utc-offset])

(defn to-jident [ident]
  (PersonIdent. (:name ident)
                (:email ident)
                (.getTime (:date ident))
                (:utc-offset ident)))

(defn load [ident]
  (->Identity (.getName ident)
              (.getEmailAddress ident)
              (.getWhen ident)
              (.getTimeZoneOffset ident)))

(defn from-map [m]
  (->Identity (:name m)
              (:email m)
              (Date.)
              (.. (Calendar/getInstance) (get Calendar/ZONE_OFFSET))))
