# Gitalin

> *"Experts call for caution over Gitalin"*
> &mdash; BBC News (not really)

gitalin is an object store for Clojure based on Git. Its data model
is simple: there are

* references (branches and tags),
* commits (= transactions),
* classes (groups of objects),
* objects (entities with (almost) arbitrary properties).

All of the above can be queried using a language similar to Datalog
and modified using a transaction interface.

Other notable characteristics include:

1. Easy setup: all it needs is an empty Git repository.
2. Easy export/backup/restore by cloning and pushing elsewhere.
3. Possibility to inspect data using Git's command-line interface.

**Note: This is work in progress. Not everything described here will work.**

## Status

* Done: Basic query interface
* Done: Basic transaction interface
* TODO: Update/remove mutations.
* TODO: Temporary IDs.
* TODO: Direct entity access - what format to return?
* TODO: Consider dropping classes as a first-class concept.
* TODO: Allow multiple connections to be used in a single query and
* TODO: Functions, not just pattern clauses.
* TODO: Tagging.
* TODO: Validation and error handling.
  allow clauses with connections like `[[?conn ?ref :ref/name ...]]`
  in combination with `:in [?conn ?other-conn]`. This would allow to
  query across multiple repositories.

### Future ideas

* Pull API
* Signed transactions

## Quick start

Clone Gitalin:

```
git clone https://github.com/jannis/gitalin
cd gitalin
```

Start a REPL:

```
boot repl
boot.user=> (gitalin.core/create-store! "/tmp/test-store")
boot.user=> (def store (gitalin.adapter/adapter "/tmp/test/store"))
boot.user=> (def conn (gitalin.core/connect store))
```

Now you are ready to create objects using transactions:

```
boot.user=> (def id (gitalin.core/tempid))
boot.user=>
boot.user=> (gitalin.core/transact! conn
boot.user=>   {:target "HEAD"
boot.user=>    :author {:name "Your Name" :email "your@email.org"}
boot.user=>    :committer {:name "Your Name" :email "your@email.org"}
boot.user=>    :message "Create John"}
boot.user=>   [[:object/add "person" id :person/name "John"]
boot.user=>    [:object/update id :person/email "john@email.org"]])
```

You can also query references, commits, classes and objects using
the query interface:

```
boot.user=> (gitalin.core/q conn
boot.user=>   {:find [?n ?e]
boot.user=>    :where [[?ref :ref/name "HEAD"]
boot.user=>            [?ref :ref/commit ?commit]
boot.user=>            [?commit :commit/class ?class]
boot.user=>            [?class :class/object ?o]
boot.user=>            [?o :person/name ?n]
boot.user=>            [?o :person/email ?e]]})
#{["John" "john@email.org"]}
```

You already know which commit `HEAD` is on and you only want
Clarice's email address? Even better:

```
boot.user=> (gitalin.core/q conn
boot.user=>   {:find ?e
boot.user=>    :in [?commit ?name]
boot.user=>    :where [[?commit :commit/class ?class]
boot.user=>            [?class :class/name "person"]
boot.user=>            [?class :class/object ?o]
boot.user=>            [?o :person/name ?name]
boot.user=>            [?o :person/email ?e]]}
boot.user=>   "commit/09a9202377d81198d409391ca54376d9c3eaadf2"
boot.user=>   "Clarice")
#{"clarice@her-domain.com"}
```

You want to know the IDs of all objects in the second most recent
transaction in `HEAD`?

```
boot.user=> (gitalin.core/q conn
boot.user=>   {:find ?o
boot.user=>    :where [[?ref :ref/name "HEAD"]
boot.user=>            [?ref :ref/commit ?commit]
boot.user=>            [?commit :commit/parent ?parent]
boot.user=>            [?parent :commit/class ?class]
boot.user=>            [?class :class/object ?o]]})
#{"object/09a9202377d81198d409391ca54376d9c3eaadf2/person/5de37b78-bcb7-482f-bff9-d8dd113a8583"
  "object/09a9202377d81198d409391ca54376d9c3eaadf2/post/d91b867e-1ba0-449b-a106-3489927b6803"}
```

There are various ways on how to improve and shorten queries.

## Query interface

TODO

## Direct entity access

TODO

## Transaction interface

TODO

## License

Copyright (c) 2015 Jannis Pohlmann <jannis@xfce.org>

Licensed under [GNU LGPL v2.1](http://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html).
