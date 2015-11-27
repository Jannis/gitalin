(ns gitalin.query
  (:import [clojure.lang PersistentVector])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [gitalin.git.coerce :refer [to-oid]]
            [gitalin.git.commit :as commit]
            [gitalin.git.ident :as ident]
            [gitalin.git.repo :as r]
            [gitalin.git.reference :as reference]
            [gitalin.git.tree :as tree]
            [gitalin.objects :as objects]
            [gitalin.protocols :as p]))

;;;; Debug helpers

(defmacro debug
  [context & body]
  `(when (-> ~context :conn :debug)
     (println ~@body)))

(defmacro debug-pprint
  [context & body]
  `(when (-> ~context :conn :debug)
     (pprint ~@body)))

;;;; Query representation

(defrecord Query [find where])

(defrecord Variable [symbol])
(defrecord Constant [value])
(defrecord Pattern [elements])

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

(defn parse-find [form]
  (or (parse-variable form)
      (parse-seq parse-variable form)))

(defn parse-in [form]
  (let [in (or (parse-variable form)
               (parse-seq parse-variable form))]
    (cond-> in
      (not (sequential? in)) vector)))

(defn parse-pattern-element [form]
  (or (parse-variable form)
      (parse-constant form)))

(defn parse-pattern [form]
  (when (vector? form)
    (Pattern. (parse-seq parse-pattern-element form))))

(defn parse-clauses [form]
  (parse-seq parse-pattern form))

(defn parse-where [form]
  (parse-clauses form))

(defn parse-query [q]
  (map->Query {:find (parse-find (:find q))
               :in (parse-in (:in q))
               :where (parse-where (:where q))}))

;;;; Execution context and variable lookup

(defrecord Context [conn bindings deps])

(defrecord BindingValue [value source-entity])

(defn var-bound? [context var]
  {:pre [(instance? Context context)
         (variable? var)]}
  (contains? (:bindings context) var))

(defn get-binding [context var]
  {:pre [(instance? Context context)
         (variable? var)]}
  ((:bindings context) var))

(defn get-values [context var]
  {:pre [(instance? Context context)
         (variable? var)]}
  (mapv :value (get-binding context var)))

;;;; Variable dependencies

(defn gather-pattern-dependencies [deps pattern]
  (when (instance? Pattern pattern)
    (let [[id property value] (:elements pattern)]
      (if (and (variable? id)
               (variable? value))
        (update deps id conj value)
        deps))))

(defn gather-clause-dependencies [deps clause]
  (or (gather-pattern-dependencies deps clause)))

(defn gather-dependencies [clauses]
  {:pre [(vector? clauses)]}
  (reduce gather-clause-dependencies {} clauses))

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
         (or (variable? property)
             (constant? property))]}
  (or (variable? property)
      (let [props (p/properties entity)]
        (not (empty? (filter #(= (:value property) (first %))
                             props))))))

(defn gather-property-value
  [context entity prop]
  {:pre [(satisfies? p/IEntity entity)
         (vector? prop)
         (= 2 (count prop))
         (keyword? (first prop))]}
  (BindingValue. (second prop) entity))

(defn gather-entity-values [context entity properties]
  {:pre [(instance? Context context)
         (satisfies? p/IEntity entity)]}
  (let [props (p/properties entity)
        props (let [possible-props (map :value properties)]
                (filter #(some #{(first %)} possible-props) props))]
    (mapv #(gather-property-value context entity %) props)))

(defn gather-values [context entities prop-or-props]
  {:pre [(instance? Context context)
         (vector? entities)
         (every? #(satisfies? p/IEntity %) entities)]}
  (into []
        (apply concat
               (mapv #(gather-entity-values context % prop-or-props)
                     entities))))

(defn has-property-value? [entity properties values]
  {:pre [(satisfies? p/IEntity entity)
         (every? #(instance? BindingValue %) properties)
         (every? #(instance? BindingValue %) values)]}
  (let [possible-props (map :value properties)
        possible-values (map :value values)]
    (some (fn [[prop val]]
            (and (some #{prop} possible-props)
                 (some #{val} possible-values)))
          (p/properties entity))))

(defn gather-entity-properties [context entity]
  {:pre [(instance? Context context)
         (satisfies? p/IEntity entity)]}
  (let [props (p/properties entity)]
    (mapv #(BindingValue. (first %) entity) props)))

(defn gather-properties [context entities]
  {:pre [(instance? Context context)
         (vector? entities)
         (every? #(satisfies? p/IEntity %) entities)]}
  (into []
        (apply concat
               (mapv #(gather-entity-properties context %)
                     entities))))

(defn update-dependency [new-entities context dep]
  (debug context "PATTERN update dependency" (:symbol dep))
  {:pre [(coll? new-entities)
         (instance? Context context)
         (variable? dep)]}
  (if (var-bound? context dep)
    (let [values (get-binding context dep)
          values-for-entities (filterv (fn [value]
                                         (or (nil? (:source-entity value))
                                             (some
                                              #{(:source-entity value)}
                                              new-entities)))
                                       values)]
      (debug context "PATTERN new values:")
      (debug-pprint context values-for-entities)
      (-> context
          (assoc-in [:bindings dep] values-for-entities)))
    context))

(defn update-dependencies [context new-entities deps]
  {:pre [(instance? Context context)
         (coll? new-entities)
         (every? variable? deps)]}
  (reduce #(update-dependency new-entities %1 %2) context deps))

(defn maybe-update-binding [context var values]
  {:pre [(instance? Context context)
         (every? #(instance? BindingValue %) values)]}
  (if (variable? var)
    (do
      (debug context "PATTERN update binding" (:symbol var))
      (debug context "PATTERN new values:")
      (debug-pprint context values)
      (let [deps ((:deps context) var)
            new-entities (map :source-entity values)]
        (-> context
            (assoc-in [:bindings var] values)
            (update-dependencies new-entities deps))))
    context))

(defn resolve-id [context id load-one-fn load-all-fn]
  {:pre [(instance? Context context)
         (or (constant? id)
             (variable? id))]}
  (debug context "PATTERN id" (element-str id))
  (debug context "PATTERN id bound?"
         (and (variable? id)
              (var-bound? context id)))
  (debug context "PATTERN id value"
         (when (variable? id)
           (get-values context id)))
  (let [adapter (p/adapter (:conn context))]
    (if (variable? id)
      (if (var-bound? context id)
        (->> (get-values context id)
             (mapv #(load-one-fn adapter %)))
        (load-all-fn adapter))
      [(load-one-fn adapter (:value id))])))

(defn resolve-properties [context property entities]
  {:pre [(instance? Context context)
         (or (constant? property)
             (variable? property))]}
  (debug context "PATTERN property" property)
  (debug context "PATTERN property bound?"
         (and (variable? property)
              (var-bound? context property)))
  ;; (debug context "PATTERN bindings:")
  ;; (debug-pprint context (:bindings context))
  (if (variable? property)
    (if (var-bound? context property)
      (get-binding context property)
      (gather-properties context entities))
    [(BindingValue. (:value property) nil)]))

(defn resolve-values [context value properties entities]
  {:pre [(instance? Context context)
         (or (constant? value)
             (variable? value))
         (every? #(instance? BindingValue %) properties)]}
  (debug context "PATTERN value" value)
  (debug context "PATTERN value var bound?"
         (and (variable? value)
              (var-bound? context value)))
  ;; (debug context "PATTERN bindings:")
  ;; (debug-pprint context (:bindings context))
  (if (variable? value)
    (if (var-bound? context value)
      (get-binding context value)
      (gather-values context entities properties))
    [(BindingValue. (:value value) nil)]))

(defn process-pattern*
  [context pattern load-one-fn load-all-fn]
  (debug context "-------")
  (debug context "PATTERN" (pattern-str pattern))
  (debug context "-------")
  (let [[id property value] (:elements pattern)
        adapter (p/adapter (:conn context))
        ;; Resolve id (var or constant) into entities
        entities (resolve-id context id load-one-fn load-all-fn)
        _ (debug context "PATTERN entities")
        _ (debug-pprint context entities)
        ;; Drop entities that don't have the property (var or constant)
        entities (filterv #(has-property? % property) entities)
        _ (debug context "PATTERN entities with" (element-str property))
        _ (debug-pprint context entities)
        ;; Gather allowed properties
        properties (resolve-properties context property entities)
        _ (debug context "PATTERN properties")
        _ (debug-pprint context properties)
        ;; Gather allowed values
        values (resolve-values context value properties entities)
        _ (debug context "PATTERN values")
        _ (debug-pprint context values)
        ;; Drop entities that don't match the allowed values
        entities (filterv #(has-property-value? % properties values)
                          entities)
        _ (debug context "PATTERN entities with matching properties")
        _ (debug-pprint context entities)
        entity-values (mapv #(BindingValue. (:id %) %) entities)]
    (-> context
        (maybe-update-binding id entity-values)
        (maybe-update-binding property properties)
        (maybe-update-binding value values))))

(defmulti process-pattern dispatch-on-property-base)

(defmethod process-pattern :ref
  [context pattern]
  (process-pattern* context pattern p/reference p/references))

(defmethod process-pattern :commit
  [context pattern]
  (process-pattern* context pattern p/commit p/commits))

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
    (let [val (into #{} (get-values context var-or-vars))]
      (debug context "COLLECT val")
      (debug-pprint context val)
      val)
    (if (coll? var-or-vars)
      (let [vals (mapv #(get-values context %) var-or-vars)]
        (assert (apply = (map count vals)))
        (debug context "COLLECT vals")
        (debug-pprint context vals)
        (into #{} (apply mapv vector vals))))))

;;;; Entry point

(defn q [conn q args]
  {:pre [(satisfies? p/IConnection conn)
         (map? q)]}
  (debug {:conn conn})
  (debug {:conn conn} "q" q args)
  (let [q (parse-query q)
        ins (zipmap (:in q)
                    (mapv #(vector (BindingValue. % nil)) args))
        deps (gather-dependencies (:where q))
        _ (debug {:conn conn} "dependencies")
        _ (debug-pprint {:conn conn} deps)
        results (reduce process-clause
                        (Context. conn ins deps)
                        (:where q))]
    (debug results "RESULTS")
    (debug-pprint results results)
    (collect (:find q) results)))
