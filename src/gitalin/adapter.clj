(ns gitalin.adapter
  (:require [clojure.string :as str]
            [gitalin.git.coerce :refer [to-oid]]
            [gitalin.git.commit :as commit]
            [gitalin.git.ident :as ident]
            [gitalin.git.repo :as git-repo]
            [gitalin.git.reference :as reference]
            [gitalin.git.tree :as tree]
            [gitalin.objects :as objects]
            [gitalin.protocols :as p])
  (:import (gitalin.git.commit Commit)
           (gitalin.git.reference Reference)
           (gitalin.objects GitalinObject)))

;;;; Temporary IDs

(defrecord TempId [uuid])

(defn tempid []
  (TempId. (str (java.util.UUID/randomUUID))))

(defn tempid? [id]
  (instance? TempId id))

;;;; Transaction context

(defrecord TransactionContext [repo tree tempids])

;;;; Transactions

(defmulti mutate-step (fn [_ mutation] (first mutation)))

(defmethod mutate-step :object/add
  [context [_ uuid property value]]
  (let [repo (:repo context)
        tree (:tree context)
        real-uuid (if (tempid? uuid)
                    (:uuid uuid)
                    uuid)
        new-tempids (if (tempid? uuid)
                      (assoc (:tempids context) uuid real-uuid)
                      (:tempids context))
        object (objects/make real-uuid {property value})
        blob (objects/make-blob repo object)
        entry (objects/to-tree-entry object blob)
        new-tree (tree/update-entry repo (:tree context) entry)]
    (-> context
        (assoc :tree new-tree)
        (assoc :tempids new-tempids))))

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

;;;; Conversion between objects and IDs

(defn obj->id [obj]
  (cond
    (instance? Reference obj)
    (str "reference/" (:name obj))

    (instance? Commit obj)
    (str "commit/" (:sha1 obj))

    (instance? GitalinObject obj)
    (str "object/" (-> obj meta :commit :sha1)
         "/" (:uuid obj))

    :else
    nil))

(defn id->obj [repo id]
  (let [segments (str/split id #"/")]
    (case (first segments)
      "reference"
      (let [[_ name] segments]
        (reference/load repo name))

      "commit"
      (let [[_ sha1] segments]
        (commit/load repo (to-oid repo sha1)))

      "object"
      (let [[_ sha1 uuid] segments
            commit' (commit/load repo (to-oid repo sha1))
            tree (commit/tree repo commit')
            object (objects/load repo tree uuid)]
        (vary-meta object assoc :commit commit'))

      :else
      nil)))

;;;; Entities

(defrecord Entity [id properties]
  p/IEntity
  (id [this]
    id)
  (properties [this]
    properties))

(defn finalize-props [props]
  (into [] (filter #(not (nil? (second %))) props)))

(defn reference->entity [repo ref]
  (let [props [[:ref/name (:name ref)]
               [:ref/commit (some->> ref :head obj->id)]
               [:ref/type (:type ref)]]]
    (Entity. (obj->id ref) (finalize-props props))))

(defn commit->entity [repo com]
  (let [tree (commit/tree repo com)
        parents (some->> (:parents com)
                         (map #(to-oid repo %))
                         (map #(commit/load repo %))
                         (mapv obj->id))
        objects (some->> (objects/load-all repo tree)
                         (map #(vary-meta % assoc :commit com))
                         (map obj->id))
        props [[:commit/sha1 (:sha1 com)]
               [:commit/author (:author com)]
               [:commit/committer (:committer com)]
               [:commit/message (:message com)]]]
    (Entity. (obj->id com)
             (finalize-props
              (concat props
                      (mapv #(vector :commit/parent %) parents)
                      (mapv #(vector :commit/object %) objects))))))

(defn object->entity [repo object]
  (let [com (:commit (meta object))
        props [[:object/uuid (:uuid object)]
               [:object/commit (obj->id com)]]]
    (Entity. (obj->id object)
             (finalize-props
              (concat props
                      (mapv (fn [[prop val]] [prop val])
                            (:properties object)))))))

;;;; Loading objects from Git

(defn collect-commits [repo commit]
  (flatten
   (into [commit]
         (mapv (fn [sha1]
                 (->> sha1
                      (to-oid repo)
                      (commit/load repo)
                      (collect-commits repo)))
               (:parents commit)))))

(defn collect-objects [repo commit']
  (let [tree (commit/tree repo commit')]
    (map (fn [object]
           (-> object
               (vary-meta assoc :commit commit')))
         (objects/load-all repo tree))))

(defn load-all-references [repo]
  (reference/load-all repo))

(defn load-all-commits [repo]
  (->> (load-all-references repo)
       (map :head)
       (map #(collect-commits repo %))
       (apply concat)
       (set)))

(defn load-all-objects [repo]
  (->> (load-all-commits repo)
       (map #(collect-objects repo %))
       (apply concat)
       (set)))

;;;; Default adapter implementation

(defrecord Adapter [path repo]
  p/IAdapter
  (connect [this]
    (assoc this :repo (git-repo/load path)))

  (disconnect [this]
    (dissoc this :repo))

  (references [this]
    (->> (load-all-references repo)
         (map #(reference->entity repo %))
         (set)))

  (reference [this id]
    (->> (id->obj repo id)
         (reference->entity repo)))

  (commit [this id]
    (->> (id->obj repo id)
         (commit->entity repo)))

  (commits [this]
    (->> (load-all-commits repo)
         (map #(commit->entity repo %))
         (set)))

  (objects [this]
    (->> (load-all-objects repo)
         (map #(object->entity repo %))
         (set)))

  (object [this id]
    (->> (id->obj repo id)
         (object->entity repo)))

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
          result (reduce mutate-step
                         (TransactionContext. repo base-tree {})
                         mutations)]
      (->> (:tree result)
           (commit! repo info (if base [base] []))
           (update-reference! repo info)))))

(defn adapter [path]
  (Adapter. path nil))
