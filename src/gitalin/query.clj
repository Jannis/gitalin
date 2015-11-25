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

(defn variable? [x]
  (instance? Variable x))

(defn constant? [x]
  (instance? Constant x))

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

(defrecord Context [conn bindings])

(defn var-bound? [context var]
  {:pre [(instance? Context context)
         (variable? var)]}
  (contains? (:bindings context) var))

(defn get-value [context var]
  {:pre [(instance? Context context)
         (variable? var)]}
  ((:bindings context) var))

;;;; Query execution

(defn dispatch-on-property-base [context pattern]
  (let [property (second (:elements pattern))
        base (cond-> (:value property)
               (keyword? (:value property)) namespace)]
    (if (keyword? (:value property))
      (keyword base)
      base)))

(defn has-property? [entity property]
  {:pre [(satisfies? p/IEntity entity)
         (constant? property)]}
  (let [props (p/properties entity)]
    (not (empty? (filter #(= (:value property) (first %)) props)))))

(defmulti gather-property-value (fn [context prop] (first prop)))

(defmethod gather-property-value :default
  [context prop]
  (second prop))

(defn gather-entity-values [context entity property]
  {:pre [(instance? Context context)
         (satisfies? p/IEntity entity)
         (constant? property)]}
  (let [props (p/properties entity)
        props (filter #(= (:value property) (first %)) props)]
    (mapv #(gather-property-value context %) props)))

(defn gather-values [context entities property]
  {:pre [(instance? Context context)
         (vector? entities)
         (every? #(satisfies? p/IEntity %) entities)
         (constant? property)]}
  (into []
        (apply concat
               (mapv #(gather-entity-values context % property)
                     entities))))

(defn has-property-value? [entity property values]
  {:pre [(satisfies? p/IEntity entity)
         (constant? property)]}
  (some (fn [[prop val]]
          (and (= prop (:value property))
               (or (= val values)
                   (some #{val} values))))
        (p/properties entity)))

(defn process-pattern*
  [context pattern load-one-fn load-all-fn]
  (debug context "PATTERN" (pattern-str pattern))
  (let [[id property value] (:elements pattern)
        adapter (p/adapter (:conn context))
        ;; Resolve id (var or constant) into entities
        entities (if (variable? id)
                   (if (var-bound? context id)
                     (->> (get-value context id)
                          (mapv #(load-one-fn adapter %)))
                     (load-all-fn adapter))
                   [(load-one-fn adapter (:value id))])
        _ (debug context "PATTERN entities")
        _ (debug-pprint context entities)
        ;; Drop entities that don't have the property
        entities (filterv #(has-property? % property) entities)
        _ (debug context "PATTERN entities with" (element-str property))
        _ (debug-pprint context entities)
        ;; Gather allowed values
        values (if (variable? value)
                 (if (var-bound? context value)
                   (get-value context value)
                   (gather-values context entities property))
                 [(:value value)])
        _ (debug context "PATTERN values")
        _ (debug-pprint context values)
        ;; Drop entities that don't match the allowed values
        entities (filterv #(has-property-value? % property values)
                          entities)
        _ (debug context "PATTERN entities with matching properties")
        _ (debug-pprint context entities)]
    (-> context
        (update :bindings
                (fn [bindings]
                  (if (variable? id)
                    (do
                      (debug context "PATTERN update" (element-str id))
                      (assoc bindings id (mapv p/id entities)))
                    bindings)))
        (update :bindings
                (fn [bindings]
                  (if (variable? value)
                    (assoc bindings value values)
                    bindings))))))

(defmulti process-pattern dispatch-on-property-base)

(defmethod process-pattern :ref
  [context pattern]
  (process-pattern* context pattern p/reference p/references))

(defmethod process-pattern :commit
  [context pattern]
  (process-pattern* context pattern p/commit p/commits))

(defmethod process-pattern :class
  [context pattern]
  (process-pattern* context pattern p/class p/classes))

(defn process-object-pattern
  [context pattern]
  (process-pattern* context pattern p/object p/objects))

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
  (if (variable? var-or-vars)
    (let [val (set (get-value context var-or-vars))]
      (debug context "COLLECT val")
      (debug-pprint context val)
      val)
    (if (coll? var-or-vars)
      (let [vals (mapv #(get-value context %) var-or-vars)]
        (assert (apply = (map count vals)))
        (debug context "COLLECT vals")
        (debug-pprint context vals)
        (apply mapv vector vals)))))

;;;; Entry point

(defn q [conn q args]
  {:pre [(satisfies? p/IConnection conn)
         (map? q)]}
  (debug {:conn conn})
  (debug {:conn conn} "q" q args)
  (let [q (parse-query q)
        ins (zipmap (:in q) args)
        results (reduce process-clause
                        (Context. conn ins)
                        (:where q))]
    (debug results ">>" results)
    (collect (:find q) results)))
