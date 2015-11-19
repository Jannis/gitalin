(ns gitalin.store
  (:require [com.stuartsierra.component :as component]
            [gitalin.git.repo :as r]
            [gitalin.git.reference :as reference]))

;;;; Atoms

(defn reference->atoms [ref]
  (let [id (:name ref)]
    (into []
          (comp (keep #(%2))
                (map #(vector id %1 %2)))
          {:ref/name id
           :ref/commit (-> ref :head :sha1)
           :ref/type (:type ref)})))

(defn repo->atoms [repo]
  (->> repo
       reference/load-all
       (map reference->atoms)
       (into [])))
