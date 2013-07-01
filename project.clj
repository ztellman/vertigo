(defproject vertigo "0.1.0-SNAPSHOT"
  :description "peering down into the machine"
  :license {:name "MIT Public License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[potemkin "0.3.1-SNAPSHOT"]
                 [primitive-math "0.1.2"]
                 [byte-streams "0.1.1-SNAPSHOT"]]
  :profiles {:dev {:dependencies [[criterium "0.4.1"]
                                  [org.clojure/clojure "1.5.1"]]}}
  :warn-on-reflection true
  :test-selectors {:default #(not (some #{:benchmark}
                                        (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :all (constantly true)}
  :jvm-opts ^:replace ["-server"])
