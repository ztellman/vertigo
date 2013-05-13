(defproject vertigo "0.1.0-SNAPSHOT"
  :description "dizzying heights of performance"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[potemkin "0.2.3-SNAPSHOT"]
                 [robert/hooke "1.3.0"]]
  :profiles {:dev {:dependencies [[criterium "0.4.1"]
                                  [org.clojure/clojure "1.5.1"]]}}
  :warn-on-reflection true
  :jvm-opts ^:replace ["-server"]
  :java-source-paths ["src"]
  :javac-options ["-target" "1.5" "-source" "1.5"]
  )
