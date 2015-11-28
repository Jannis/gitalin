(ns gitalin.objects
  (:import [java.util UUID]
           [org.eclipse.jgit.lib FileMode])
  (:refer-clojure :exclude [load])
  (:require [gitalin.git.coerce :refer [to-oid]]
            [gitalin.git.blob :as blob]
            [gitalin.git.commit :as commit]
            [gitalin.git.tree :as git-tree]
            [gitalin.transit :refer [transit-read transit-write]]))

(defrecord GitalinObject [uuid properties])

(defn load-from-entry [repo entry]
  (let [uuid  (:name entry)
        oid   (to-oid repo (:sha1 entry))
        blob  (blob/load repo oid)
        props (transit-read (:data blob))]
    (->GitalinObject uuid props)))

(defn load-all [repo tree]
  (some->> tree
           :entries
           (filter #(= :file (:type %)))
           (map #(load-from-entry repo %))))

(defn load [repo tree uuid]
  (some->> tree
           :entries
           (filter #(= :file (:type %)))
           (filter #(= uuid (:name %)))
           (first)
           (load-from-entry repo)))

(defn make [uuid properties]
  (->GitalinObject uuid properties))

(defn make-blob [repo object]
  (blob/write repo (-> (:properties object)
                       (transit-write)
                       (.getBytes "utf-8"))))

(defn to-tree-entry [object blob]
  (git-tree/->TreeEntry (:uuid object)
                        (:sha1 blob)
                        :file))
