(ns gitalin.git.repo
  (:import [org.eclipse.jgit.api InitCommand]
           [org.eclipse.jgit.revwalk RevWalk]
           [org.eclipse.jgit.treewalk TreeWalk])
  (:refer-clojure :exclude [load])
  (:require [clojure.java.io :as io]
            [clj-jgit.porcelain :refer [load-repo]]))

;;;; Loading repositories

(defn load [location]
  (load-repo location))

(defn init [location]
  (-> (InitCommand.)
      (.setDirectory (io/as-file location))
      (.call))
  (load location))

;;;; Utilities for walking revisions and trees

(defn rev-walk [repo]
  (RevWalk. (.getRepository repo)))

(defn tree-walk
  [repo trees]
  (let [tw (TreeWalk. (.getRepository repo))]
    (doseq [t trees] (.addTree tw t))
    tw))

(defn tree-walk-for-entry [repo tree path]
  (TreeWalk/forPath (.getRepository repo) path tree))

;;;; Utilities for loading and creating objects

(defn object-loader [repo oid]
  (.open (.getRepository repo) oid))

(defn object-inserter [repo]
  (.newObjectInserter (.getRepository repo)))

(defn object-type [repo oid]
  (.getType (object-loader repo oid)))
