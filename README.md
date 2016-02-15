# Gitalin

> *"Experts call for caution over Gitalin"*
> &mdash; BBC News (not really)

gitalin is an object store for Clojure based on Git. Its data model
is simple: there are

* references (branches and tags),
* commits (= transactions),
* objects (entities with (almost) arbitrary properties).

All of the above can be queried using a language similar to Datalog
and modified using a transaction interface.

Other notable characteristics include:

1. Easy setup: all it needs is an empty Git repository.
2. Easy export/backup/restore by cloning and pushing elsewhere.
3. Possibility to inspect data using Git's command-line interface.

**Note: This is work in progress. Not everything described here will work.**

## Status

[![Build Status](https://travis-ci.org/Jannis/gitalin.svg?branch=master)](https://travis-ci.org/Jannis/gitalin)

* Done: Basic query interface
* Done: Basic transaction interface
* TODO: Update/remove mutations.
* TODO: Temporary IDs.
* TODO: Direct entity access - what format to return?
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
```

Create a store and connect to it:

```
(use '[gitalin.core :as g])

(g/create-store! "/tmp/test-store")

(def store (g/default-adapter "/tmp/test-store"))
(def conn (g/connect store))
```

Now you are ready to create objects using transactions:

```
(def id (g/tempid))

(let [id (g/tempid)]
  (g/transact! conn
               {:target "HEAD"
                :author {:name "Your Name" :email "your@email.org"}
                :committer {:name "Your Name" :email "your@email.org"}
                :message "Create John"}
               [[:object/add id :person/name "John"]
                [:object/set id :person/email "john@email.org"]]))
```

You can also query references, commits and objects using the query
interface:

```
(g/q conn '{:find [?n ?e]
            :where [[?ref :ref/name "HEAD"]
                    [?ref :ref/commit ?commit]
                    [?commit :commit/object ?o]
                    [?o :person/name ?n]
                    [?o :person/email ?e]]})

-> #{["John" "john@email.org"]}
```

You already know which commit `HEAD` is on and you only want
Clarice's email address? Even better:

```
(g/q conn
     '{:find ?e
       :in [?commit ?name]
       :where [[?commit :commit/object ?o]
               [?o :person/name ?name]
               [?o :person/email ?e]]}
      "commit/09a9202377d81198d409391ca54376d9c3eaadf2"
      "Clarice")

-> #{"clarice@her-domain.com"}
```

You want to know the IDs of all objects in the second most recent
transaction in `HEAD`?

```
(g/q conn '{:find ?o
            :where [[?ref :ref/name "HEAD"]
                    [?ref :ref/commit ?commit]
                    [?commit :commit/parent ?parent]
                    [?parent :commit/object ?o]]})

-> #{"object/09a9202377d81198d409391ca54376d9c3eaadf2/5de37b78-bcb7-482f-bff9-d8dd113a8583"
     "object/09a9202377d81198d409391ca54376d9c3eaadf2/d91b867e-1ba0-449b-a106-3489927b6803"}
```

You can also query for all objects that ever existed in the store:

```
(g/q conn '{:find ?o
            :where [[?o :object/id ?u]]})

-> #{"object/09a9202377d81198d409391ca54376d9c3eaadf2/5de37b78-bcb7-482f-bff9-d8dd113a8583"
"object/09a9202377d81198d409391ca54376d9c3eaadf2/d91b867e-1ba0-449b-a106-3489927b6803"
"object/c0cb7699274b80eb585062666a2922f5d4082913/e2b3e246-ac12-4d7a-82a1-349d4e8fdf89"}
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
