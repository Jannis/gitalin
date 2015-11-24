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
      (throw "Not Implemented Yet")

      "commit"
      (throw "Not Implemented Yet")

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

(defn reference-obj->atoms [ref]
  (let [id (obj->id ref)
        props {:ref/name (:name ref)
               :ref/commit (obj->id (:head ref))
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

(defn commit-obj->atoms [repo commit']
  (let [id (obj->id commit')
        props {:commit/sha1 (:sha1 commit')
               :commit/author (:author commit')
               :commit/committer (:committer commit')
               :commit/message (:message commit')}
        atoms (->> props
                   (filter kv-value-set?)
                   (map #(into [id] %)))
        tree (commit/tree repo commit')
        parent-atoms (some->> (:parents commit')
                              (map #(to-oid repo %))
                              (map #(commit/load repo %))
                              (map obj->id)
                              (mapv #(vector id :commit/parent %)))
        class-atoms (some->> (classes/load-all repo tree)
                             (map #(vary-meta % assoc :commit commit'))
                             (map obj->id)
                             (mapv #(vector id :commit/class %)))]
    (concat atoms parent-atoms class-atoms)))

(defn class-obj->atoms [repo class]
  (let [commit' (:commit (meta class))
        tree (commit/tree repo commit')
        id (obj->id class)
        props {:class/name (:name class)
               :class/commit (obj->id commit')}
        atoms (->> props
                   (filter kv-value-set?)
                   (map #(into [id] %)))
        object-atoms (some->> (:objects class)
                              (map :uuid)
                              (map #(objects/load repo tree class %))
                              (map #(vary-meta % assoc :commit commit'))
                              (map obj->id)
                              (mapv #(vector id :class/object %)))]
    (concat atoms object-atoms)))

(defn object-obj->atoms [object]
  (let [commit (:commit (meta object))
        id (obj->id object)
        props {:object/uuid (:uuid object)
               :object/class (:class object)
               :object/commit (obj->id commit)}
        atoms (->> props
                   (filter kv-value-set?)
                   (map #(into [id] %)))
        property-atoms (some->> (:properties object)
                                (map (fn [[prop val]]
                                       (vector id prop val))))]
    (concat atoms property-atoms)))

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

  (reference->atoms [this id]
    (let [name (second (str/split id #"/" 2))]
      (reference-obj->atoms (reference/load repo name))))

  (references->atoms [this]
    (let [references (reference/load-all repo)]
      (into [] (apply concat (map reference-obj->atoms references)))))

  (commit->atoms [this id]
    (let [sha1 (second (str/split id #"/" 2))
          oid (to-oid repo sha1)]
      (commit-obj->atoms repo (commit/load repo oid))))

  (commits->atoms [this]
    (->> (load-all-commits repo)
         (map #(commit-obj->atoms repo %))
         (apply concat)
         (set)))

  (classes->atoms [this]
    (->> (load-all-classes repo)
         (map #(class-obj->atoms repo %))
         (apply concat)
         (set)))

  (class->atoms [this id]
    (->> (id->obj repo id)
         (class-obj->atoms repo)))

  (objects->atoms [this]
    (->> (load-all-objects repo)
         (map object-obj->atoms)
         (apply concat)
         (set)))

  (object->atoms [this id]
    (println "object->atoms" id)
    (->> (id->obj repo id)
         (object-obj->atoms)))

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
                  (map #(commit-obj->atoms repo %) commits)
                  (map class->atoms classes)))))
