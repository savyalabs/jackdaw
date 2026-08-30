(defproject net.clojars.savya/jackdaw "1.4.1"
  :plugins [[lein-tools-deps "0.4.5"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]
                           :aliases [:test]}
  :test-selectors {:default (complement :integration)
                   :integration :integration
                   :all (constantly true)})
