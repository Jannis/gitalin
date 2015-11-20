(ns gitalin.classes
  (:refer-clojure :exclude [load])
  (:require [gitalin.git.coerce :refer [to-oid]]
            [gitalin.git.commit :as commit]
            [gitalin.git.tree :as git-tree]))

(defrecord GitalinClass [name objects])

(defn load-from-entry [repo entry]
  (let [name    (:name entry)
        oid     (to-oid repo (:sha1 entry))
        tree    (git-tree/load repo oid)
        objects (map (fn [e] {:uuid (:name e)}) (:entries tree))]
    (->GitalinClass name objects)))

(defn load-all [repo tree]
  (some->> (:entries tree)
           (filter #(= :tree (:type %)))
           (map (partial load-from-entry repo))
           (map #(-> [(:name %) %]))
           (into {})))

(defn load [repo tree name]
  (some-> (load-all repo tree)
          (get name)))

(defn tree [repo tree class]
  (some->> (:entries tree)
           (filter #(= (:name class) (:name %)))
           (first)
           :sha1
           (to-oid repo)
           (git-tree/load repo)))

(defn load-for-uuid [repo tree uuid]
  (some->> (load-all repo tree)
           (vals)
           (filter #(some #{{:uuid uuid}} (:objects %)))
           (first)))
