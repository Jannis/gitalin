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

;;;; Debug helpers

(defmacro debug
  [context & body]
  `(when (-> ~context :conn :debug)
     (println ~@body)
     (flush)))

(defmacro debug-pprint
  [context & body]
  `(when (-> ~context :conn :debug)
     (pprint ~@body)
     (flush)))

;;;; Query representation

(defrecord Query [find where])

(defrecord Variable [symbol])
(defrecord Constant [value])
(defrecord Pattern [elements])
(defrecord Function [symbol args])

;;;; Query printing

(defn variable-str [var]
  (when (instance? Variable var)
    (:symbol var)))

(defn constant-str [constant]
  (when (instance? Constant constant)
    (:value constant)))

(defn element-str [element]
  (or (variable-str element)
      (constant-str element)))

(defn pattern-str [pattern]
  {:pre [(instance? Pattern pattern)]}
  (str (mapv element-str (:elements pattern))))

(defn var-str [var-or-vars]
  (if (instance? Variable var-or-vars)
    (variable-str var-or-vars)
    (mapv var-str var-or-vars)))

;;;; Query parsing

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

;;;; Execution context and variable lookup

(defrecord Context [conn vars matches])

(defn resolve-var-in-context [context var]
  {:pre [(instance? Context context)
         (instance? Variable var)]}
  ((:vars context) (:symbol var)))

(defn resolve-var-in-atom [var atom]
  {:pre [(instance? Variable var)
         (satisfies? p/ICoatom atom)]}
  (let [atom-vars (:vars (meta atom))]
    (first
     (keep-indexed (fn [index atom-var]
                     (when (= var atom-var)
                       (atom index)))
                   atom-vars))))

(defn resolve-var-in-atoms [context atoms var]
  {:pre [(instance? Context context)
         (every? #(satisfies? p/ICoatom %) atoms)
         (instance? Variable var)]}
  (let [vals (into []
                   (comp (map #(resolve-var-in-atom var %))
                         (remove nil?))
                   atoms)
        res (if (empty? vals) nil vals)]
    res))

(defn resolve-var [context var]
  {:pre [(instance? Context context)
         (instance? Variable var)]}
  (let [res (or (resolve-var-in-context context var)
                (resolve-var-in-atoms context (:matches context) var))]
    res))

(defn resolve-vars [context vars]
  {:pre [(instance? Context context)
         (every? #(instance? Variable %) vars)]}
  (mapv #(resolve-var context %) vars))

;;;; Query execution

(defn matches-variable? [context element value]
  (and (instance? Variable element)
       (let [var-value (resolve-var context element)]
         (or (nil? var-value)
             (if (coll? var-value)
               (or (some #{value} var-value)
                   (= value var-value))
               (= var-value value))))))

(defn matches-constant? [element value]
  (and (instance? Constant element)
      (= (:value element) value)))

(defn matches-element? [context element value]
  (or (matches-variable? context element value)
      (matches-constant? element value)))

;; (defn match-fields-against-pattern [context pattern atom]
;;   (doall
;;    (map-indexed (fn [index element]
;;                   (if (matches-element? context element (atom index))
;;                     element
;;                     nil))
;;                 (:elements pattern))))

(defn match-fields-against-pattern [context pattern atom]
  {:pre [(instance? Context context)
         (instance? Pattern pattern)
         (satisfies? p/ICoatom atom)]}
  (let [elements (:elements pattern)]
    (if (matches-element? context (elements 0) (p/id atom))
      (map-indexed (fn [index element]
                     (if (matches-element? context element (atom index))
                       element
                       nil))
                   elements)
      (take (count elements) (constantly nil)))))

(defn collect-match-vars [field-matches]
  (mapv (fn [element]
          (if (instance? Variable element)
            element
            nil))
        field-matches))

(defn match-atom-against-pattern [pattern context res atom]
  {:pre [(instance? Pattern pattern)
         (instance? Context context)
         (map? res)
         (satisfies? p/ICoatom atom)]}
  (let [matches (match-fields-against-pattern context pattern atom)
        vars (collect-match-vars matches)]
    (if (not-any? nil? matches)
      (update res :matches conj (vary-meta atom assoc :vars vars))
      (update res :rejects conj atom))))

(defn match-atoms-against-pattern [context pattern atoms]
  {:pre [(instance? Context context)
         (instance? Pattern pattern)
         (every? #(satisfies? p/ICoatom %) atoms)]}
  (reduce #(match-atom-against-pattern pattern context %1 %2)
          {:matches [] :rejects []}
          atoms))

(defn resolve-id [context id]
  (let [resolved-id (or (when (instance? Variable id)
                          (resolve-var context id))
                        id)]
    (cond
      (instance? Variable resolved-id) resolved-id
      (instance? Constant resolved-id) (:value resolved-id)
      :else resolved-id)))

(defn dispatch-on-property-base [context pattern]
  (let [property (second (:elements pattern))
        base (cond-> (:value property)
               (keyword? (:value property)) namespace)]
    (if (keyword? (:value property))
      (keyword base)
      base)))

(defn consolidate-matches [old new rejects]
  (->> (into old new)
       (remove #(some #{%} rejects))))

(defn process-pattern* [context pattern one->atoms many->atoms]
  (debug context "PATTERN" (pattern-str pattern))
  (debug context "-------")
  (let [id-or-ids (resolve-id context (first (:elements pattern)))
        adapter (p/adapter (:conn context))
        atoms (if (instance? Variable id-or-ids)
                (many->atoms adapter)
                (if (coll? id-or-ids)
                  (->> id-or-ids
                       (map #(one->atoms adapter %))
                       (apply concat))
                  (one->atoms adapter id-or-ids)))
        res (match-atoms-against-pattern context pattern atoms)
        matches (:matches res)
        rejects (:rejects res)
        new-context (update context :matches
                            consolidate-matches
                            matches rejects)]
    (debug context "BEFORE")
    (debug-pprint context (:matches context))
    (debug context "MATCHES")
    (debug-pprint context matches)
    (debug context "REJECTS")
    (debug-pprint context rejects)
    (debug context "AFTER")
    (debug-pprint new-context (:matches new-context))
    new-context))

(defmulti process-pattern dispatch-on-property-base)

(defmethod process-pattern :ref
  [context pattern]
  (process-pattern* context
                    pattern
                    p/reference->atoms
                    p/references->atoms))

(defmethod process-pattern :commit
  [context pattern]
  (process-pattern* context
                    pattern
                    p/commit->atoms
                    p/commits->atoms))

(defmethod process-pattern :class
  [context pattern]
  (process-pattern* context
                    pattern
                    p/class->atoms
                    p/classes->atoms))

(defn process-object-pattern
  [context pattern]
  (process-pattern* context
                    pattern
                    p/object->atoms
                    p/objects->atoms))

(defmethod process-pattern :object
  [context pattern]
  (process-object-pattern context pattern))

(defmethod process-pattern :default
  [context pattern]
  (process-object-pattern context pattern))

(defn process-pattern-clause [context clause]
  (when (instance? Pattern clause)
    (process-pattern context clause)))

(defn process-clause [context clause]
  (or (process-pattern-clause context clause)))

;;;; Collecting query results

(defn collect [var-or-vars context]
  (debug context "COLLECT" (var-str var-or-vars))
  (let [val-or-vals (if (sequential? var-or-vars)
                      (mapv #(resolve-var context %) var-or-vars)
                      (resolve-var context var-or-vars))
        _ (debug context "  VAL OR VALS >>" val-or-vals)
        vectorized (if (sequential? var-or-vars)
                     (apply mapv vector val-or-vals)
                     val-or-vals)
        _ (debug context "  VECTORIZED >>" vectorized)
        res (if (nil? vectorized)
              #{}
              (if (coll? vectorized)
                (into #{} vectorized)
                #{vectorized}))]
    (debug context "COLLECT <<" res)
    res))

;;;; Entry point

(defn q [conn q args]
  {:pre [(satisfies? p/IConnection conn)
         (map? q)]}
  (debug {:conn conn})
  (debug {:conn conn} "q" q args)
  (let [q (parse-query q)
        ins (zipmap (:in q) args)
        results (reduce process-clause
                        (Context. conn ins [])
                        (:where q))]
    (debug results ">>" results)
    (collect (:find q) results)))
