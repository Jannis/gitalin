(ns gitalin.query
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
            [gitalin.objects :as objects]
            [gitalin.protocols :as p]))

(comment 
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
                    (map class->atoms classes))))))

;;;; Transactions

(comment 
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
           (update-reference! repo info)))))

;;;; Queries

;;; Query parsing

(defrecord Variable [symbol])
(defrecord Constant [value])
(defrecord Pattern [elements])
(defrecord Function [symbol args])
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

(defn parse-in [form]
  (or (:symbol (parse-variable form))
      (map :symbol (parse-seq parse-variable form))))

(defn parse-pattern-element [form]
  (or (parse-variable form)
      (parse-constant form)))

(defn parse-pattern [form]
  (when (vector? form)
    (Pattern. (parse-seq parse-pattern-element form))))

(defn parse-function-argument [form]
  (or (parse-variable form)
      (parse-constant form)))

(defn parse-function-arguments [form]
  (when (sequential? form)
    (parse-seq parse-function-argument form)))

(defn parse-function [form]
  (when (and (seq? form) (symbol? (first form)))
    (let [args (parse-function-arguments (rest form))]
      (Function. (first form) args))))

(defn parse-clause [form]
  (or (parse-pattern form)
      (parse-function form)))

(defn parse-clauses [form]
  (parse-seq parse-clause form))

(defn parse-where [form]
  (or (parse-clauses form)))

(defn parse-query [q]
  (map->Query {:find (parse-find (:find q))
               :in (parse-in (:in q))
               :where (parse-where (:where q))}))

;;; Query execution

(defrecord Context [conn relations matches])

(defn collapse-relations [relations more]
  (merge-with (fn [a b]
                (cond
                  (= a b) a
                  :else   (conj a b)))
              relations
              more))

(defn atom-matches-id? [id' atom]
  (or (instance? Variable id')
      (= id' (p/id atom))))

(defn matches-variable? [element value]
  (instance? Variable element))

(defn matches-constant? [element value]
  (and (instance? Constant element)
      (= (:value element) value)))

(defn matches-element? [element value]
  (or (matches-variable? element value)
      (matches-constant? element value)))

(defn matches-pattern? [pattern atom]
  (when (instance? Pattern pattern)
    (and (matches-element? ((:elements pattern) 0) (p/id atom))
         (matches-element? ((:elements pattern) 1) (p/property atom))
         (matches-element? ((:elements pattern) 2) (p/value atom)))))

(defn filter-atoms [pattern atoms]
  {:pre [(instance? Pattern pattern)]}
  (filterv #(matches-pattern? pattern %) atoms))

(defn collect-atom-relations [pattern relations atom]
  {:pre [(instance? Pattern pattern)]}
  (let [vals (map-indexed (fn [index element]
                            (when (instance? Variable element)
                              [(:symbol element) (atom index)]))
                          (:elements pattern))
        rels (reduce (fn [rels [var value]]
                       (if (contains? rels var)
                         (if (sequential? (rels var))
                           (update rels var conj value)
                           (if (= value (rels var))
                             rels
                             (update rels var vector value)))
                         (assoc rels var value)))
                     relations
                     (remove nil? vals))]
    (println "collect atom relations")
    (println "  pattern >>" pattern)
    (println "  atom    >>" atom)
    (println "  rels    >>" rels)
    rels))

(defn collect-relations [pattern atoms]
  {:pre [(instance? Pattern pattern)]}
  (reduce #(collect-atom-relations pattern %1 %2) {} atoms))

(defn lookup-relation [context var]
  (when (instance? Variable var)
    ((:relations context) (:symbol var))))

(defn dispatch-on-property-base [context pattern]
  (let [property (second (:elements pattern))
        base (cond-> (:value property)
               (keyword? (:value property)) namespace)]
    (if (keyword? (:value property))
      (keyword base)
      base)))

(defmulti resolve-pattern dispatch-on-property-base)

(defmethod resolve-pattern :ref
  [context pattern]
  (let [id (first (:elements pattern))
        atoms (if (instance? Variable id)
                (p/references->atoms (-> context :conn p/adapter))
                (p/reference->atoms (-> context :conn p/adapter) id))
        ; TODO: atoms (map #(into [(-> context :conn p/conn-id)]))
        matches (filter-atoms pattern atoms)
        relations (collect-relations pattern matches)]
    (println "relations" relations)
    (-> context
        (update :relations collapse-relations relations)
        (update :matches concat matches))))

(defn resolve-pattern-clause [context clause]
  (println "resolve-pattern-clause" clause)
  (when (instance? Pattern clause)
    (resolve-pattern context clause)))

;; (defn evaluate-function [function atom]
;;   false)

;; (defn matches-function? [function atom]
;;   (when (instance? Function function)
;;     (println "matches-function?" function atom)
;;     (and (instance? Function function)
;;          (evaluate-function function atom))))

(defn dispatch-on-function-symbol [context function]
  (:symbol function))

(defn resolve-function-args [context args]
  (map (fn [arg]
         (cond
           (instance? Variable arg)
           (lookup-relation context arg)

           (instance? Constant arg)
           (:value arg)

           :else
           arg))
       args))

(defmulti resolve-function dispatch-on-function-symbol)

(defmethod resolve-function 'some
  [context function]
  (println "resolve-function 'some" function)
  (let [args (resolve-function-args context (:args function))]
    (println "  args >>" args)
    context))

(defn resolve-function-clause [context clause]
  (println "resolve-function-clause" clause)
  (when (instance? Function clause)
    (resolve-function context clause)))

(defn resolve-clause [context clause]
  (or (resolve-pattern-clause context clause)
      (resolve-function-clause context clause)))

(defn collect [vars context]
  (println "collect" vars context)
  (cond
    ;; (set? vars)
    ;; (mapv #(lookup-relation context %) vars)

    (sequential? vars)
    (if (> (count (:matches context)) 1)
      (map-indexed (fn [index atom]
                     (mapv #((lookup-relation context %) index) vars))
                   (:matches context))
      (mapv #(lookup-relation context %) vars))

    :else
    (lookup-relation context vars)))

(defn q [conn q args]
  {:pre [(satisfies? p/IConnection conn)
         (map? q)]}
  (println)
  (println "q" q args)
  (let [q (parse-query q)
        ins (if (:in q)
              (zipmap (if (sequential? (:in q))
                        (:in q)
                        [(:in q)])
                      args)
              {})
        _ (println "ins" ins)
        results (reduce resolve-clause
                        (Context. conn ins [])
                        (:where q))]
    (println "results >>" results)
    (collect (:find q) results)))
