(ns gitalin.objects
  (:import [java.util UUID]
           [org.eclipse.jgit.lib FileMode])
  (:refer-clojure :exclude [load])
  (:require [gitalin.git.coerce :refer [to-oid]]
            [gitalin.git.blob :as blob]
            [gitalin.git.commit :as commit]
            [gitalin.git.tree :as git-tree]
            [gitalin.classes :as classes]
            [gitalin.transit :refer [transit-read transit-write]]))

(defrecord GitalinObject [uuid class properties])

(defn load-from-entry [repo class entry]
  (let [uuid  (:name entry)
        oid   (to-oid repo (:sha1 entry))
        blob  (blob/load repo oid)
        props (transit-read (:data blob))]
    (->GitalinObject uuid (:name class) props)))

(defn load-all [repo tree class]
  (some->> (classes/tree repo tree class)
           :entries
           (filter #(= :file (:type %)))
           (map (partial load-from-entry repo class))))

(defn load [repo tree class uuid]
  (some->> (classes/tree repo tree class)
           :entries
           (filter #(= :file (:type %)))
           (filter #(= uuid (:name %)))
           (first)
           (load-from-entry repo class)))

(defn make [uuid class properties]
  (->GitalinObject uuid class properties))

(defn make-blob [repo object]
  (blob/write repo (-> (:properties object)
                       (transit-write)
                       (.getBytes "utf-8"))))

(defn to-tree-entry [object blob]
  (git-tree/->TreeEntry (:uuid object)
                        (:sha1 blob)
                        :file))
