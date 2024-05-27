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

(rtest/launch-module! ipc WordCountModule {:tasks 4 :threads 2})

(def sentences-depot (foreign-depot ipc (get-module-name WordCountModule) "*sentences-depot"))
(def word-counts (foreign-pstate ipc (get-module-name WordCountModule) "$$word-counts"))

(comment
  (foreign-append! sentences-depot "Hello world")
  (foreign-append! sentences-depot "hello hello goodbye")
  (foreign-append! sentences-depot "Alice says hello")

  (foreign-select-one (keypath "hello") word-counts) ; => 4
  (foreign-select-one (keypath "goodbye") word-counts) ; => 1

  (close! ipc)
  :rcf)

;; 在 module 以外也可以调用 dataflow 的各种 api, partitioner 除外
(comment
  (?<-
   (println "Hello, world!"))

  (deframaop foo [*a]
    (:> (inc *a))
    (:> (dec *a)))
  
  (?<-
   (foo 5 :> *v)
   (println *v))
  
  ;; <<if 看成是 if 就可以了
  ;; else> 看成是 else
  (?<-
   (<<if (= 1 2)
         (println "true branch 1")
         (println "true branch 2")
         (else>)
         (println "else branch 1")
         (println "else branch 2")))
  
  (deframaop my-filter [*v]
    (<<if *v
          (:>)))
  
  (?<-
   (ops/range> 0 5 :> *v)
   (my-filter (even? *v))
   (println *v))

  :rcf)

