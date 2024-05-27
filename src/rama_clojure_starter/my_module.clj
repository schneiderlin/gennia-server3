(ns rama-clojure-starter.my-module
  (:use [com.rpl.rama]
        [com.rpl.rama.path])
  (:require
   [clojure.string :as str]
   [com.rpl.rama.test :as rtest]
   [com.rpl.rama.aggs :as aggs]
   [com.rpl.rama.ops :as ops]))

;; *something means variable in Rama dataflow
;; <<sources just like a datalow block macro, no specific semantic
;; source> subscribe to depot

(defmodule WordCountModule [setup topologies]
  (declare-depot setup *sentences-depot :random)
  (let [s (stream-topology topologies "word-count")]
    (declare-pstate s $$word-counts {String Long})
    (<<sources s
               (source> *sentences-depot :> *sentence)
               (str/split (str/lower-case *sentence) #" " :> *words)
               (ops/explode *words :> *word)
               (|hash *word)
               (+compound $$word-counts {*word (aggs/+count)}))))

(def ipc (rtest/create-ipc))


