(defproject vertigo "0.1.0"
  :description "peering down into the machine"
  :license {:name "MIT Public License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[potemkin "0.3.1"]
                 [primitive-math "0.1.2"]
                 [byte-streams "0.1.1"]]
  :profiles {:dev {:dependencies [[criterium "0.4.1"]
                                  [org.clojure/clojure "1.5.1"]
                                  [codox-md "0.2.0" :exclusions [org.clojure/clojure]]]}
             :benchmark {:jvm-opts ["-Dvertigo.unsafe"]}}
  :plugins [[codox "0.6.4"]]
  :codox {:writer codox-md.writer/write-docs
		  :exclude [vertigo.primitives]}
  :warn-on-reflection true
  :test-selectors {:default #(not (some #{:benchmark}
                                        (cons (:tag %) (keys %))))
                   :benchmark :benchmark
                   :all (constantly true)}
  :aliases {"benchmark" ["with-profile" "dev,benchmark" "test" ":benchmark"]}
  :jvm-opts ^:replace ["-server"])
