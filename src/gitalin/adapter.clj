(ns gitalin.adapter
  (:require [clojure.string :as str]
            [gitalin.git.coerce :refer [to-oid]]
            [gitalin.git.commit :as commit]
            [gitalin.git.ident :as ident]
            [gitalin.git.repo :as git-repo]
            [gitalin.git.reference :as reference]
            [gitalin.git.tree :as tree]
            [gitalin.classes :as classes]
            [gitalin.objects :as objects]
            [gitalin.protocols :as p]))

;;;; Helpers

(defn kv-value-set? [[k v]]
  (not (nil? v)))

;;;; Default adapter

(defmulti mutate-step (fn [_ _ mutation] (first mutation)))

(comment
  ;; How about something like this?
  (defmethod mutate-step :commit/add)
  (defmethod mutate-step :ref/update))

(defmethod mutate-step :object/add
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

(defn reference-obj->atoms [ref]
  (let [id (str "reference/" (:name ref))
        props {:ref/name (:name ref)
               :ref/commit (str "commit/" (-> ref :head :sha1))
               :ref/type (:type ref)}]
    (->> props
         (filter kv-value-set?)
         (map #(into [id] %)))))

(defn collect-commits [repo commit]
  (flatten
   (into [commit]
         (mapv (fn [sha1]
                 (->> sha1
                      (to-oid repo)
                      (commit/load repo)
                      (collect-commits repo)))
               (:parents commit)))))

(defn commit-obj->atoms [commit]
  (let [id (str "commit/" (:sha1 commit))
        props {:commit/sha1 (:sha1 commit)
               :commit/author (:author commit)
               :commit/committer (:committer commit)
               :commit/message (:message commit)}
        atoms (->> props
                   (filter kv-value-set?)
                   (map #(into [id] %)))
        parent-atoms (some->> (:parents commit)
                              (mapv #(vector id :commit/parent %)))]
    (concat atoms parent-atoms)))

(defrecord Adapter [path repo]
  p/IAdapter
  (connect [this]
    (assoc this :repo (git-repo/load path)))

  (disconnect [this]
    (dissoc this :repo))

  (reference->atoms [this id]
    (let [name (second (str/split id #"/" 2))]
      (reference-obj->atoms (reference/load repo name))))

  (references->atoms [this]
    (let [references (reference/load-all repo)]
      (into [] (apply concat (map reference-obj->atoms references)))))

  (commit->atoms [this id]
    (let [sha1 (second (str/split id #"/" 2))
          oid (to-oid repo sha1)]
      (commit-obj->atoms (commit/load repo oid))))

  (commits->atoms [this]
    (->> (reference/load-all repo)
         (map :head)
         (map #(collect-commits repo %))
         (apply concat)
         (map commit-obj->atoms)
         (apply concat)
         (set)))

  (transact! [this info mutations]
    (let [base (if (:base info)
                 (commit/load repo (to-oid repo (:base info)))
                 (when (:target info)
                   (some->> (reference/load repo (:target info))
                            :head :sha1 (to-oid repo)
                            (commit/load repo))))
          base-tree (if base
                      (commit/tree repo base)
                      (tree/make-empty repo))
          tree (reduce (partial mutate-step repo) base-tree mutations)]
      (->> tree
           (commit! repo info (if base [base] []))
           (update-reference! repo info)))))

(defn adapter [path]
  (Adapter. path nil))

;;;; Generate atoms for the entire repository at once

(defn class->atoms [class]
  [])

(defn collect-classes [repo commit]
  (classes/load-all repo (commit/tree repo commit)))

(defn repo->atoms [adapter]
  (let [repo (:repo adapter)
        references (reference/load-all repo)
        commits (->> references
                     (map :head)
                     (map #(collect-commits repo %))
                     (apply concat))
        classes (->> commits
                     (map #(collect-classes repo %))
                     (apply concat))]
    (into []
          (mapcat concat
                  (map reference-obj->atoms references)
                  (map commit-obj->atoms commits)
                  (map class->atoms classes)))))
