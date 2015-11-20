(ns gitalin.store
  (:import [clojure.lang PersistentVector])
  (:require [com.stuartsierra.component :as component]
            [gitalin.git.coerce :refer [to-oid]]
            [gitalin.git.commit :as commit]
            [gitalin.git.ident :as ident]
            [gitalin.git.repo :as r]
            [gitalin.git.reference :as reference]
            [gitalin.git.tree :as tree]
            [gitalin.objects :as objects]))

;;;; Atoms

(defprotocol ICoAtom
  (id [this])
  (property [this])
  (value [this]))

(extend-type PersistentVector
  ICoAtom
  (id [this]
    (this 0))
  (property [this]
    (this 1))
  (value [this]
    (this 2)))

(defn kv-value-set? [[k v]]
  (not (nil? v)))

(defn reference->atoms [ref]
  (let [id (:name ref)
        props {:ref/name id
               :ref/commit (-> ref :head :sha1)
               :ref/type (:type ref)}]
    (->> props
         (filter kv-value-set?)
         (map #(into [id] %)))))

(defn collect-commits [repo commit]
  (concat [commit]
          (map #(collect-commits repo %)
               (:parents commit))))

(defn commit->atoms [commit]
  (let [id (:sha1 commit)
        props {:commit/sha1 (:sha1 commit)
               :commit/author (:author commit)
               :commit/committer (:committer commit)
               :commit/message (:message commit)}
        atoms (->> props
                   (filter kv-value-set?)
                   (map #(into [id] %)))]
    (if-not (empty? (:parents commit))
      (conj atoms (map #(vector id :commit/parent %) parents))
      atoms)))

(defn repo->atoms [repo]
  (let [references (reference/load-all repo)
        commits (->> references
                     (map :head)
                     (map #(collect-commits repo %))
                     (apply concat))]
    (into []
          (mapcat concat
                  (map reference->atoms references)
                  (map commit->atoms commits)))))

(defn commit-info? [info]
  (and (map? info)
       (contains? info :target)))

(defmulti mutate-step (fn [_ _ mutation] (first mutation)))

(defmethod mutate-step :store/add
  [repo base [_ class uuid property value]]
  (let [tree (or (tree/get-tree repo base class)
                 (tree/make-empty repo))
        object (objects/make uuid class {property value})
        blob (objects/make-blob repo object)
        entry (objects/to-tree-entry object blob)]
    (->> entry
         (tree/update-entry repo tree)
         (tree/to-tree-entry class)
         (tree/update-entry repo base))))

(defn commit! [repo info parents tree]
  (commit/make repo tree parents
               :author (ident/from-map (:author info))
               :committer (ident/from-map (or (:committer info)
                                              (:author info)))
               :message (:message info)))

(defn update-reference! [repo info commit]
  (when-let [target (reference/load repo (:target info))]
    (when (reference/update! repo target commit)
      (reference/load repo (:target info)))))

(defn transact! [repo info mutations]
  {:pre [(commit-info? info) (vector? mutations)]}
  (let [base (when (:base info)
               (commit/load repo (to-oid repo (:base info))))
        base-tree (if base
                    (commit/tree repo base)
                    (tree/make-empty repo))
        tree (reduce (partial mutate-step repo) base-tree mutations)]
    (->> tree
         (commit! repo info (if base [base] []))
         (update-reference! repo info))))
