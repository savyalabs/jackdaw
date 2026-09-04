(ns build
  "Build + Clojars deploy for jackdaw (tools.build + deps-deploy).

   Usage:
     clojure -T:build jar
     clojure -T:build deploy   ; needs CLOJARS_USERNAME / CLOJARS_PASSWORD"
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'net.clojars.savya/jackdaw)
(def version "1.4.1")
(def class-dir "target/classes")
(def basis (delay (b/create-basis {:project "deps.edn"})))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

;; Namespaces AOT-compiled in the published jar (gen-class / deftype serdes that
;; consumers reference by name). Everything else ships as source.
(def aot-nses '[jackdaw.serdes.edn2 jackdaw.serdes.fressian jackdaw.serdes.fn-impl])

(defn clean [_]
  (b/delete {:path "target"})
  (b/delete {:path "pom.xml"}))   ; drop stale lein-generated pom so :pom-data wins

(defn compile-aot
  "AOT-compile the gen-class serdes into target/classes. Required before
   `clojure -M:test`: several tests reference these classes by name (e.g. the
   Kafka `default.key.serde` config wants jackdaw.serdes.EdnSerde), and gen-class
   only emits the named class under AOT, not on plain require."
  [_]
  (b/compile-clj {:basis @basis
                  :ns-compile aot-nses
                  :class-dir class-dir}))

(defn jar [_]
  (clean nil)
  (b/compile-clj {:basis @basis
                  :ns-compile aot-nses
                  :class-dir class-dir})
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src"]
                :scm {:url "https://github.com/savyalabs/jackdaw"
                      :connection "scm:git:https://github.com/savyalabs/jackdaw.git"
                      :developerConnection "scm:git:ssh://git@github.com/savyalabs/jackdaw.git"
                      :tag (str "v" version)}
                :pom-data [[:description "A Clojure library for the Apache Kafka distributed streaming platform."]
                           [:url "https://github.com/savyalabs/jackdaw"]
                           [:licenses
                            [:license
                             [:name "BSD 3-clause"]
                             [:url "http://opensource.org/licenses/BSD-3-Clause"]
                             [:distribution "repo"]]]]})
  (b/copy-dir {:src-dirs ["src" "resources"] :target-dir class-dir})
  (b/jar {:class-dir class-dir :jar-file jar-file})
  (println "Wrote" jar-file))

(defn deploy [_]
  (jar nil)
  (dd/deploy {:installer :remote
              :artifact jar-file
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))
