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
            [gitalin.protocols :as p])
  (:import (gitalin.git.commit Commit)
           (gitalin.git.reference Reference)
           (gitalin.classes GitalinClass)
           (gitalin.objects GitalinObject)))

;;;; Helpers

(defn kv-value-set? [[k v]]
  (not (nil? v)))

;;;; Default adapter

;;; Entities

(defrecord Entity [id properties]
  p/IEntity
  (id [this]
    id)
  (properties [this]
    properties))

;;; Transactions

(defmulti mutate-step (fn [_ _ mutation] (first mutation)))

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

;;; Queries

(defn obj->id [obj]
  (cond
    (instance? Reference obj)
    (str "reference/" (:name obj))

    (instance? Commit obj)
    (str "commit/" (:sha1 obj))

    (instance? GitalinClass obj)
    (str "class/" (-> obj meta :commit :sha1) "/" (:name obj))

    (instance? GitalinObject obj)
    (str "object/" (-> obj meta :commit :sha1)
         "/" (:class obj)
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

      "class"
      (let [[_ sha1 name] segments
            commit' (commit/load repo (to-oid repo sha1))
            tree (commit/tree repo commit')
            class (classes/load repo tree name)]
        (vary-meta class assoc :commit commit'))

      "object"
      (let [[_ sha1 class-name uuid] segments
            commit' (commit/load repo (to-oid repo sha1))
            tree (commit/tree repo commit')
            class (classes/load repo tree class-name)
            object (objects/load repo tree class uuid)]
        (vary-meta object assoc :commit commit'))

      :else
      nil)))

(defn finalize-props [props]
  (into [] (filter #(not (nil? (second %))) props)))

(defn reference->entity [repo ref]
  (let [props [[:ref/name (:name ref)]
               [:ref/commit (some->> ref :head obj->id)]
               [:ref/type (:type ref)]]]
    (Entity. (obj->id ref) (finalize-props props))))

(defn collect-commits [repo commit]
  (flatten
   (into [commit]
         (mapv (fn [sha1]
                 (->> sha1
                      (to-oid repo)
                      (commit/load repo)
                      (collect-commits repo)))
               (:parents commit)))))

(defn commit->entity [repo com]
  (let [tree (commit/tree repo com)
        parents (some->> (:parents com)
                         (map #(to-oid repo %))
                         (map #(commit/load repo %))
                         (mapv obj->id))
        classes (some->> (classes/load-all repo tree)
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
                      (mapv #(vector :commit/class %) classes))))))

(defn class->entity [repo class]
  (let [com (:commit (meta class))
        tree (commit/tree repo com)
        objects (some->> (:objects class)
                         (map :uuid)
                         (map #(objects/load repo tree class %))
                         (map #(vary-meta % assoc :commit com))
                         (map obj->id))
        props [[:class/name (:name class)]
               [:class/commit (obj->id com)]]]
    (Entity. (obj->id class)
             (finalize-props
              (concat props
                      (mapv #(vector :class/object %) objects))))))

(defn object->entity [repo object]
  (let [com (:commit (meta object))
        props [[:object/uuid (:uuid object)]
               [:object/class (:class object)]
               [:object/commit (obj->id com)]]]
    (Entity. (obj->id object)
             (finalize-props
              (concat props
                      (mapv (fn [[prop val]] [prop val])
                            (:properties object)))))))

(defn collect-classes [repo commit']
  (let [tree (commit/tree commit')]
    (map (fn [class]
           (-> class
               (vary-meta assoc :commit commit')
               (vary-meta assoc :tree tree)))
         (classes/load-all repo tree))))

(defn collect-objects [repo class]
  (let [{:keys [commit tree]} (meta class)]
    (map (fn [object]
           (-> object
               (vary-meta assoc :commit commit)))
         (objects/load-all repo tree class))))

(defn load-all-references [repo]
  (reference/load-all repo))

(defn load-all-commits [repo]
  (->> (reference/load-all repo)
       (map :head)
       (map #(collect-commits repo %))
       (apply concat)
       (set)))

(defn load-all-classes [repo]
  (->> (load-all-commits repo)
       (map #(collect-classes repo %))
       (apply concat)
       (set)))

(defn load-all-objects [repo]
  (->> (load-all-classes repo)
       (map #(collect-objects repo %))
       (apply concat)
       (set)))

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

  (classes [this]
    (->> (load-all-classes repo)
         (map #(class->entity repo %))
         (set)))

  (class [this id]
    (->> (id->obj repo id)
         (class->entity repo)))

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
          tree (reduce (partial mutate-step repo) base-tree mutations)]
      (->> tree
           (commit! repo info (if base [base] []))
           (update-reference! repo info)))))

(defn adapter [path]
  (Adapter. path nil))
