(ns gitalin.git.tree
  (:import [org.eclipse.jgit.lib FileMode TreeFormatter])
  (:refer-clojure :exclude [load])
  (:require [gitalin.git.coerce :refer [to-file-mode to-oid to-sha1]]
            [gitalin.git.repo :refer [object-inserter
                                      rev-walk
                                      tree-walk
                                      tree-walk-for-entry]]))

(defn tree-formatter []
  (TreeFormatter.))

(defrecord TreeEntry [name sha1 type])
(defrecord Tree [sha1 entries])

(defn entries [repo jtree]
  (let [walk    (tree-walk repo [jtree])
        entries (transient [])]
    (while (.next walk)
      (conj! entries (->TreeEntry (.getPathString walk)
                                  (.getName (.getObjectId walk 0))
                                  (if (.isSubtree walk) :tree :file))))
    (persistent! entries)))

(defn to-tree [repo jtree]
  (let [sha1    (.getName jtree)
        entries (entries repo jtree)]
    (->Tree sha1 entries)))

(defn to-jtree [repo tree]
  (some->> (:sha1 tree)
           (to-oid repo)
           (.parseTree (rev-walk repo))))

(defn load [repo oid]
  (some->> (.parseTree (rev-walk repo) oid)
           (to-tree repo)))

(defn make-empty [repo]
  (let [formatter (tree-formatter)
        inserter  (object-inserter repo)
        oid       (.insert inserter formatter)]
    (do
      (.flush inserter)
      (load repo oid))))

(defn get-tree [repo tree subtree-name]
  (let [jtree (to-jtree repo tree)
        walk  (tree-walk-for-entry repo jtree subtree-name)]
    (when walk
      (load repo (.getObjectId walk 0)))))

(defn contains-entry? [tree name]
  ((complement empty?) (filter #{name} (map :name (:entries tree)))))

(defn write [repo tree]
  (let [formatter (tree-formatter)]
    (doseq [entry (:entries tree)]
      (.append formatter (:name entry)
               (to-file-mode (:type entry))
               (to-oid repo (:sha1 entry))))
    (let [inserter (object-inserter repo)
          oid      (.insert inserter formatter)]
      (.flush inserter)
      oid)))

(defn update-entry [repo tree entry]
  (->> (update tree :entries (fn [entries]
                               (conj (remove #(= (:name %) (:name entry))
                                             entries)
                                     entry)))
    (write repo)
    (load repo)))

(defn remove-entry [repo tree entry]
  (->> (update tree :entries (fn [entries]
                               (remove #(= (:name %) (:name entry))
                                       entries)))
    (write repo)
    (load repo)))

(defn to-tree-entry [name tree]
  (->TreeEntry name (:sha1 tree) :tree))
