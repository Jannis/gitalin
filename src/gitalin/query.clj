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

(defmacro debug
  [context & body]
  `(when (-> ~context :conn :debug)
     (println ~@body)
     (flush)))

;;;; Queries

;;; Query representation

(defrecord Query [find where])

(defrecord Variable [symbol])
(defrecord Constant [value])
(defrecord Pattern [elements])
(defrecord Function [symbol args])

;;; Query printing

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

(defn resolve-var-in-atoms [context atoms var]
  (debug context "    RESOLVE IN ATOMS >>" atoms var)
  (let [val (into []
                  (comp (map #(resolve-var-in-atom var %))
                        (keep identity))
                  atoms)
        res (if (empty? val)
              nil
              val)]
    (debug context "    RESOLVE IN ATOMS <<" res)
    res))

(defn resolve-var [context var]
  {:pre [(instance? Context context)
         (instance? Variable var)]}
  (debug context "  RESOLVE" (var-str var))
  (let [res (or (resolve-var-in-context context var)
                (resolve-var-in-atoms context (:matches context) var))]
    (debug context "  RESOLVE <<" res)
    res))

(defn resolve-vars [context vars]
  {:pre [(instance? Context context)
         (every? #(instance? Variable %) vars)]}
  (mapv #(resolve-var context %) vars))

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

(defn match-fields-against-pattern [context pattern atom]
  (doall
   (map-indexed (fn [index element]
                  (if (matches-element? context element (atom index))
                    element
                    nil))
                (:elements pattern))))

(defn collect-match-vars [field-matches]
  (mapv (fn [element]
          (if (instance? Variable element)
            element
            nil))
        field-matches))

(defn match-atom-against-pattern [pattern context res atom]
  (debug context "  MATCH >>" atom)
  (let [matches (match-fields-against-pattern context pattern atom)
        _ (debug context "    MATCHES >>" matches)
        vars (collect-match-vars matches)]
    (if (not-any? nil? matches)
      (do
        (debug context "  MATCH <<" atom)
        (conj res (vary-meta atom assoc :vars vars)))
      (do
        (debug context "  MATCH << nil")
        res))))

(defn match-atoms-against-pattern [context pattern atoms]
  {:pre [(instance? Pattern pattern)]}
  (reduce #(match-atom-against-pattern pattern context %1 %2)
          [] atoms))

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

(defmulti resolve-pattern dispatch-on-property-base)

(defmethod resolve-pattern :ref
  [context pattern]
  (debug context "PATTERN >>" (pattern-str pattern))
  (let [id (first (:elements pattern))
        resolved-id (resolve-id context id)
        adapter (p/adapter (:conn context))
        atoms (if (instance? Variable resolved-id)
                (p/references->atoms adapter)
                (if (coll? resolved-id)
                  (->> resolved-id
                       (map #(p/reference->atoms adapter %))
                       (apply concat))
                  (p/reference->atoms adapter resolved-id)))
        matches (match-atoms-against-pattern context pattern atoms)]
    (debug context "PATTERN <<" matches)
    (update context :matches into matches)))

(defmethod resolve-pattern :commit
  [context pattern]
  (debug context "PATTERN >>" (pattern-str pattern))
  (let [id (first (:elements pattern))
        resolved-id (resolve-id context id)
        adapter (p/adapter (:conn context))
        atoms (if (instance? Variable resolved-id)
                (p/commits->atoms adapter)
                (if (coll? resolved-id)
                  (->> resolved-id
                       (map #(p/commit->atoms adapter %))
                       (apply concat))
                  (p/commit->atoms adapter resolved-id)))
        matches (match-atoms-against-pattern context pattern atoms)]
    (debug context "PATTERN <<" matches)
    (update context :matches into matches)))

(defmethod resolve-pattern :class
  [context pattern]
  (debug context "PATTERN >>" (pattern-str pattern))
  (let [id (first (:elements pattern))
        resolved-id (resolve-id context id)
        adapter (p/adapter (:conn context))
        atoms (if (instance? Variable resolved-id)
                (p/classes->atoms adapter)
                (if (coll? resolved-id)
                  (->> resolved-id
                       (map #(p/class->atoms adapter %))
                       (apply concat))
                  (p/class->atoms adapter resolved-id)))
        matches (match-atoms-against-pattern context pattern atoms)]
    (debug context "PATTERN <<" matches)
    (update context :matches into matches)))

(defn resolve-object-pattern
  [context pattern]
  (debug context "PATTERN" (pattern-str pattern))
  (let [id (first (:elements pattern))
        resolved-id (resolve-id context id)
        adapter (p/adapter (:conn context))
        atoms (if (instance? Variable resolved-id)
                (p/objects->atoms adapter)
                (if (coll? resolved-id)
                  (->> resolved-id
                       (map #(p/object->atoms adapter %))
                       (apply concat))
                  (p/object->atoms adapter resolved-id)))
        _ (debug context "  ATOMS" atoms)
        matches (match-atoms-against-pattern context pattern atoms)]
    (debug context "PATTERN <<" matches)
    (update context :matches into matches)))

(defmethod resolve-pattern :object
  [context pattern]
  (resolve-object-pattern context pattern))

(defmethod resolve-pattern :default
  [context pattern]
  (resolve-object-pattern context pattern))

(defn resolve-pattern-clause [context clause]
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
  (debug context "FUNCTION" function)
  (letfn [(apply-some [atom]
            (let [args (resolve-args context atom (:args function))
                  res (apply some args)]
              res))]
    (update context :matches #(filter apply-some %))))

(defn resolve-function-clause [context clause]
  (when (instance? Function clause)
    (resolve-function context clause)))

(defn resolve-clause [context clause]
  (or (resolve-pattern-clause context clause)
      (resolve-function-clause context clause)))

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

;; (defn collect [var-or-vars context]
;;   (debug context "collect" var-or-vars context)
;;   (into #{}
;;         (comp
;;          (map (fn [atom]
;;                 (if (sequential? var-or-vars)
;;                   (mapv #(resolve-var-in-atom % atom) var-or-vars)
;;                   (resolve-var-in-atom var-or-vars atom))))
;;          (remove nil?))
;;         (:matches context)))

  ;; (let [res (if (sequential? var-or-vars)
  ;;             (into #{}
  ;;                   (apply map vector
  ;;                          (resolve-vars context var-or-vars)))
  ;;             (resolve-var context var-or-vars))]
  ;;   (debug context "res" res)
  ;;   res))

(defn q [conn q args]
  {:pre [(satisfies? p/IConnection conn)
         (map? q)]}
  (debug {:conn conn})
  (debug {:conn conn} "q" q args)
  (let [q (parse-query q)
        ins (zipmap (:in q) args)
        results (reduce resolve-clause
                        (Context. conn ins [])
                        (:where q))]
    (debug results ">>" results)
    (collect (:find q) results)))
