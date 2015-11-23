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

(def debug? false)

(defmacro debug
  [& body]
  `(when ~debug?
    (println ~@body)))

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
    (classes/load-all repo (commit/tree repo commit))))

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

;;; Query representation

(defrecord Query [find where])

(defrecord Variable [symbol])
(defrecord Constant [value])
(defrecord Pattern [elements])
(defrecord Function [symbol args])

;;; Query parsing

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

(defn parse-set [parse-fn form]
  (when (set? form)
    (reduce #(if-let [res (parse-fn %2)]
               (conj %1 res)
               (reduced nil))
            #{} form)))

(defn parse-find [form]
  (or (parse-variable form)
      (parse-seq parse-variable form)))

(defn parse-in [form]
  (let [in (or (:symbol (parse-variable form))
               (map :symbol (parse-seq parse-variable form)))]
    (cond-> in
      (not (sequential? in)) vector)))

(defn parse-pattern-element [form]
  (or (parse-variable form)
      (parse-constant form)))

(defn parse-pattern [form]
  (when (vector? form)
    (Pattern. (parse-seq parse-pattern-element form))))

(defn parse-function-argument [form]
  (or (parse-variable form)
      (parse-set parse-function-argument form)
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

(defrecord Context [conn vars matches])

(defn resolve-var-in-context [context var]
  ((:vars context) (:symbol var)))

(defn resolve-var-in-atom [var atom]
  (let [atom-vars (:vars (meta atom))]
    (first
     (keep-indexed (fn [index atom-var]
                     (when (= var atom-var)
                       (atom index)))
                   atom-vars))))

(defn resolve-var-in-atoms [atoms var]
  (let [val (into #{}
                  (comp (map #(resolve-var-in-atom var %))
                        (keep identity))
                  atoms)]
    (if-not (empty? val)
      (if (> (count val) 1)
        val
        (first val))
      nil)))

(defn resolve-var [context var]
  {:pre [(instance? Context context)
         (instance? Variable var)]}
  (or (resolve-var-in-context context var)
      (resolve-var-in-atoms (:matches context) var)))

(defn resolve-vars [context vars]
  {:pre [(instance? Context context)
         (every? #(instance? Variable %) vars)]}
  (mapv #(resolve-var context %) vars))

(defn matches-variable? [context element value]
  (debug "matches-variable?" element value)
  (and (instance? Variable element)
       (let [var-value (resolve-var context element)]
         (debug "  var-value >>" var-value)
         (debug "  value     >>" value)
         (or (nil? var-value)
             (if (coll? var-value)
               (or (some #{value} var-value)
                   (= value var-value))
               (= var-value value))))))

(defn matches-constant? [element value]
  (debug "matches-constant?" element value)
  (and (instance? Constant element)
      (= (:value element) value)))

(defn matches-element? [context element value]
  (debug "matches-element?" element value)
  (or (matches-variable? context element value)
      (matches-constant? element value)))

(defn match-fields-against-pattern [context pattern atom]
  (map-indexed (fn [index element]
                 (if (matches-element? context element (atom index))
                   element
                   nil))
               (:elements pattern)))

(defn collect-match-vars [field-matches]
  (mapv (fn [element]
          (if (instance? Variable element)
            element
            nil))
        field-matches))

(defn match-atom-against-pattern [pattern context res atom]
  (debug "match-atom-against-pattern" pattern res atom)
  (let [matches (match-fields-against-pattern context pattern atom)
        vars (collect-match-vars matches)]
    (debug "  matches >>" matches)
    (debug "  vars    >>" vars)
    (if (not-any? nil? matches)
      (conj res (vary-meta atom assoc :vars vars))
      res)))

(defn match-atoms-against-pattern [context pattern atoms]
  {:pre [(instance? Pattern pattern)]}
  (debug "match-atoms-against-pattern" pattern atoms)
  (reduce #(match-atom-against-pattern pattern context %1 %2)
          #{} atoms))

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
  (debug "resolve-pattern" pattern)
  (let [id (first (:elements pattern))
        atoms (if (instance? Variable id)
                (p/references->atoms (-> context :conn p/adapter))
                (p/reference->atoms (-> context :conn p/adapter)
                                    (:value id)))
        matches (match-atoms-against-pattern context pattern atoms)
        ; TODO: atoms (map #(into [(-> context :conn p/conn-id)]))
        ]
    (debug "new matches >>" matches)
    (update context :matches into matches)))

(defn resolve-pattern-clause [context clause]
  (debug "resolve-pattern-clause" clause)
  (when (instance? Pattern clause)
    (resolve-pattern context clause)))

(defn dispatch-on-function-symbol [context function]
  (:symbol function))

(defn resolve-arg [context atom arg]
  (cond
    (instance? Variable arg) (resolve-var-in-atom arg atom)
    (set? arg) (into #{} (map #(resolve-arg context atom %)) arg)
    (instance? Constant arg) (:value arg)
    :else arg))

(defn resolve-args [context atom args]
  (map #(resolve-arg context atom %) args))

(defmulti resolve-function dispatch-on-function-symbol)

(defmethod resolve-function 'some
  [context function]
  (debug "resolve-function 'some" function)
  (letfn [(apply-some [atom]
            (debug "apply-some" atom)
            (let [args (resolve-args context atom (:args function))
                  res (apply some args)]
              (debug "  args >>" args)
              (debug "  res  >>" res)
              res))]
    (debug "  matches >>" (:matches context))
    (debug "  result  >>")
    (update context :matches #(filter apply-some %))))

(defn resolve-function-clause [context clause]
  (debug "resolve-function-clause" clause)
  (when (instance? Function clause)
    (resolve-function context clause)))

(defn resolve-clause [context clause]
  (or (resolve-pattern-clause context clause)
      (resolve-function-clause context clause)))

(defn collect [var-or-vars context]
  (debug "collect" var-or-vars context)
  (let [res (if (sequential? var-or-vars)
              (into #{}
                    (apply map vector
                           (resolve-vars context var-or-vars)))
              (resolve-var context var-or-vars))]
    (debug "res" res)
    (if (= (count res) 1)
      (first res)
      res)))

(defn q [conn q args]
  {:pre [(satisfies? p/IConnection conn)
         (map? q)]}
  (debug)
  (debug "q" q args)
  (let [q (parse-query q)
        ins (zipmap (:in q) args)
        results (reduce resolve-clause
                        (Context. conn ins #{})
                        (:where q))]
    (debug "results >>" results)
    (collect (:find q) results)))
