(ns gitalin.store
  (:import [clojure.lang PersistentVector])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [gitalin.git.coerce :refer [to-oid]]
            [gitalin.git.commit :as commit]
            [gitalin.git.ident :as ident]
            [gitalin.git.repo :as r]
            [gitalin.git.reference :as reference]
            [gitalin.git.tree :as tree]
            [gitalin.classes :as classes]
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

(defn class->atoms [class]
  [])

(defn collect-commits [repo commit]
  (concat [commit]
          (map #(collect-commits repo %)
               (:parents commit))))

(defn collect-classes [repo commit]
  (classes/load-all repo (commit/tree repo commit)))

(defn repo->atoms [repo]
  (let [references (reference/load-all repo)
        commits (->> references
                     (map :head)
                     (map #(collect-commits repo %))
                     (apply concat))
        classes (->> commits
                     (map #(collect-classes repo %))
                     (apply concat))]
    (into []
          (mapcat concat
                  (map reference->atoms references)
                  (map commit->atoms commits)
                  (map class->atoms classes)))))

;;;; Transactions

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

(defn commit-info? [info]
  (and (map? info)
       (contains? info :target)))

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

;;;; Queries

;;; Query parsing

(defrecord Variable [symbol])
(defrecord Constant [value])
(defrecord Query [find where])

(defn parse-variable [form]
  (when (and (symbol? form)
             (= (first (name form)) \?))
    (Variable. form)))

(defn parse-constant [form]
  (when-not (symbol? form)
    (Constant. form)))

(defn parse-seq [parse-fn form]
  (when (sequential? form)
    (reduce #(if-let [res (parse-fn %2)]
               (conj %1 res)
               (reduced nil))
            [] form)))

(defn parse-find [form]
  (or (parse-variable form)
      (parse-seq parse-variable form)))

(defn parse-pattern-element [form]
  (or (parse-variable form)
      (parse-constant form)))

(defn parse-pattern [form]
  (parse-seq parse-pattern-element form))

(defn parse-clause [form]
  (or (parse-pattern form)))

(defn parse-clauses [form]
  (parse-seq parse-clause form))

(defn parse-where [form]
  (or (parse-clauses form)))

(defn parse-query [q]
  (map->Query {:find (parse-find (:find q))
               :where (parse-where (:where q))}))

;;; Query execution

(defrecord Context [repo relations])

(defn collapse-relations [relations more]
  (merge-with merge relations more))

(defn atom-matches-id? [id' atom]
  (or (instance? Variable id')
      (= id' (id atom))))

(defn matches-variable? [element value]
  (instance? Variable element))

(defn matches-constant? [element value]
  (and (instance? Constant element)
      (= (:value element) value)))

(defn matches-element? [element value]
  (or (matches-variable? element value)
      (matches-constant? element value)))

(defn matches-clause? [clause atom]
  (and (matches-element? (clause 0) (id atom))
       (matches-element? (clause 1) (property atom))
       (matches-element? (clause 2) (value atom))))

(defn filter-atoms [clause atoms]
  (filterv #(matches-clause? clause %) atoms))

(defn collect-atom-relations [clause relations atom]
  (let [vals (map-indexed (fn [index element]
                            (when (instance? Variable element)
                              [(:symbol element) (atom index)]))
                          clause)
        rels (reduce (fn [rels [var value]]
                       (if (contains? rels var)
                         (if (sequential? (rels var))
                           (update rels var conj value)
                           (update rels var vector value))
                         (assoc rels var value)))
                     {}
                     (remove nil? vals))]
    rels))

(defn collect-relations [clause atoms]
  (reduce #(collect-atom-relations clause %1 %2)
          #{}
          atoms))

(defn dispatch-on-property-base [context clause]
  (let [property (second clause)
        base (cond-> (:value property)
               (keyword? (:value property)) namespace)]
    (if (keyword? (:value property))
      (keyword base)
      base)))

(defmulti resolve-clause dispatch-on-property-base)

(defmethod resolve-clause :ref
  [context clause]
  (let [id (first clause)
        atoms (if (instance? Variable id)
                (let [refs (reference/load-all (:repo context))]
                  (apply concat (map reference->atoms refs)))
                (let [ref (reference/load (:repo context) id)]
                  (reference->atoms ref)))
        matches (filter-atoms clause atoms)
        relations (collect-relations clause matches)]
    (update context :relations collapse-relations relations)))

(defn lookup-relation [context var]
  (when (instance? Variable var)
    ((:relations context) (:symbol var))))

(defn collect [vars context]
  (if (sequential? vars)
    (mapv #(lookup-relation context %) vars)
    (lookup-relation context vars)))

(defn q [repo q & args]
  (let [q (parse-query q)
        results (reduce resolve-clause
                        (Context. repo {})
                        (:where q))]
    (collect (:find q) results)))
