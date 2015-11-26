#!/usr/bin/env boot

(set-env!
 :source-paths #{"test"}
 :resource-paths #{"src"}
 :dependencies '[;; Boot
                 [adzerk/boot-test "1.0.5" :scope "test"]

                 ;; Testing
                 [org.clojure/test.check "0.9.0" :scope "test"]
                 [me.raynes/fs "1.4.6" :scope "test"]

                 ;; General
                 [clj-jgit "0.8.8"]
                 [com.cognitect/transit-clj "0.8.281"]])

(task-options!
 pom {:project 'gitalin
      :version "0.1.0-SNAPSHOT"})

(require '[adzerk.boot-test :refer :all])
