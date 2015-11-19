(ns gitalin.git.commit
  (:import [org.eclipse.jgit.lib CommitBuilder])
  (:refer-clojure :exclude [load])
  (:require [gitalin.git.coerce :refer [to-oid to-sha1]]
            [gitalin.git.ident :as ident]
            [gitalin.git.repo :refer [object-inserter rev-walk]]
            [gitalin.git.tree :as git-tree]))

(defn commit-builder []
  (CommitBuilder.))

(defrecord Commit [sha1
                   author
                   committer
                   subject
                   message
                   parents])

(defn to-commit [repo jcommit]
  (let [sha1      (.getName jcommit)
        author    (ident/load (.getAuthorIdent jcommit))
        committer (ident/load (.getCommitterIdent jcommit))
        subject   (.getShortMessage jcommit)
        message   (.getFullMessage jcommit)
        parents   (mapv #(to-sha1 (.getId %)) (.getParents jcommit))]
    (->Commit sha1 author committer subject message parents)))

(defn load [repo oid]
  (some->> (.parseCommit (rev-walk repo) oid)
           (to-commit repo)))

(defn to-jcommit [repo commit]
  (some->> (:sha1 commit)
           (to-oid repo)
           (.parseCommit (rev-walk repo))))

(defn tree [repo commit]
  (some->> (to-jcommit repo commit)
           (.getTree)
           (.getId)
           (git-tree/load repo)))

(defn make [repo tree parents & {:keys [author committer message]}]
  (let [builder (commit-builder)]
    (.setTreeId builder (to-oid repo (:sha1 tree)))
    (doseq [parent parents]
      (.addParentId builder (to-oid repo (:sha1 parent))))
    (.setAuthor builder (ident/to-jident author))
    (.setCommitter builder (ident/to-jident committer))
    (.setMessage builder message)
    (let [inserter (object-inserter repo)
          oid      (.insert inserter builder)]
      (.flush inserter)
      (load repo oid))))
