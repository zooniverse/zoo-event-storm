(ns zoo-storm.bolts.gendercode
  (:require [backtype.storm.clojure :refer [bolt emit-bolt! ack! defbolt]]
            [clojure.string :as str]
            [clojure.java.io :refer :all])
  (:gen-class))

(defn open-file
  [file]
  (-> file resource .toURI reader))

(defn process-data
  [prefix]
  (fn [names line]
    (let [[name prob & rest] (str/split line #"\s+")]
      (if (names name)
        (update-in names [name] assoc prefix (Double. prob))
        (assoc names name {prefix (Double. prob)})))))

(defn to-prob
  [[name prob]]
  (let [mp (or (:mp prob) 0)
        fp (or (:fp prob) 0)]
    [name (/ mp (+ mp fp))]))

(defn init-data
  []
  (with-open [female-list (open-file "dist.female.first")
              male-list (open-file "dist.male.first")]
    (into {} 
          (map to-prob
               (reduce (process-data :mp)
                       (reduce (process-data :fp) {} (line-seq female-list)) 
                       (line-seq male-list))))))

(defn code-name
  [names name]
  (let [[fname] (str/split name #"\s+")]
    (if-let [fname-prob (names (str/upper-case fname))]
      {:assigned (if (> fname-prob (rand)) "m" "f")
       :male fname-prob
       :female (- 1 fname-prob)}
      {:assigned "u"
       :male -1
       :female -1})))

(defbolt gendercode-event ["event" "type" "project"] {:prepare true}
  [conf context collector]
  (let [gender-fn (partial code-name (init-data))] 
    (bolt
      (execute [{:strs [event type project] :as tuple}] 
               (let [new-tuple (merge event (gender-fn (:name event)))]
                 (emit-bolt! collector [new-tuple type project] :anchor tuple)
                 (ack! collector tuple))))))
