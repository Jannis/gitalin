(ns gitalin.git.blob
  (:import [org.eclipse.jgit.lib Constants])
  (:refer-clojure :exclude [load])
  (:require [gitalin.git.coerce :refer [to-sha1]]
            [gitalin.git.repo :refer [object-inserter object-loader]]))

(defrecord Blob [sha1 data])

(defn load [repo oid]
  (some->> (object-loader repo oid)
           (.getBytes)
           (->Blob (to-sha1 oid))))

(defn write [repo data]
  (let [inserter (object-inserter repo)
        oid      (.insert inserter (Constants/OBJ_BLOB) data)]
    (.flush inserter)
    (->Blob (to-sha1 oid) data)))
