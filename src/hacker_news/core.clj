(ns hacker-news.core
  (:require [org.httpkit.client :as http]
            [cheshire.core :as json])
  (:gen-class))


(def top-stories-url "https://hacker-news.firebaseio.com/v0/topstories.json")
(def item-req-api "https://hacker-news.firebaseio.com/v0/item/")

(def n-stories 30)
(def n-commenters 10)

(declare item-url do-item parse-json get-item parse-resp watcher)


(defn -main
    "Get top n stories, print story titles and n commenters of these stories"
    [& args]

    (println "hello!!!")

    (let [s-agent (agent [] :error-handler  #(println %2) :error-mode :continue)  ;; Agent for stories
          c-agent (agent {} :error-handler  #(println %2) :error-mode :continue)  ;; Agent for commenters counts
          r-agent (agent 0  :error-handler  #(println %2) :error-mode :continue)]  ;; Agent for counting active http request

        (add-watch r-agent :active-requests watcher) ; watcher for changes of request-count agent

        ;; Get n top-stories and n top-commenters
        (->> (parse-resp @(http/get top-stories-url))
             (take n-stories)
             (map (partial do-item s-agent c-agent r-agent))
             (doall))

        ;; Wait for all active http requests to complete
        (loop [count @r-agent]
          (if (= count 0)
              count
             (recur @r-agent)))

        (await s-agent c-agent r-agent) ;; wait for all actions on all agents to complete

        (println "TOP STORIES") (println (apply str (interpose "\n" (deref s-agent))) "\n")
        (println "TOP COMMENTERS") (println (apply str (interpose "\n" (take n-commenters (deref c-agent)))) "\n")

    )

    (shutdown-agents) ;shutdown agent thread pool to terminate the JVM

    (println "THE END...")
)



(defn do-item
    "http/get story,  return future of asynch request"
    [story-a comment-a count-a id]

    (send count-a inc) ;; Count one more active request

    (let [url (item-url id)]
      (http/get url {:timeout 60000 :sa story-a :pa comment-a :ca count-a}
                ;; asynchronous response call back
                (fn [{:keys [status headers body error opts]}]

                  (send (:ca opts) dec) ;; Decrease one less active request

                  (if error
                    (do (println url " -> timeout") error)
                    (let [{:keys [type title by kids]} (parse-json body)
                          inc-freq (fn [freqs k] (->> (get freqs k 0) (inc) (assoc freqs k)))]

                      (if (= type "story")
                         (send (:sa opts)  #(conj %1 %2) title) ;; update list of stories tittle
                         (send (:pa opts)  inc-freq by))        ;; count frequencies of commenters

                      ;; Send http requests for all children items
                      (doall (map (partial do-item (:sa opts) (:pa opts) (:ca opts)) kids))
                      )))
    )))



(defn item-url
    "Construct a http request for an item"
    [id]
    (str item-req-api id ".json"))


(defn get-item
    "Send a http request for an item id, return the json response"
    [id]
    @(http/get (item-url id)))

(defn parse-json
    "Convert json to clojure collection"
    [json]
    (json/parse-string json true))


(defn parse-resp
    "Convert http json response to clojure map. If error, print error and return {} "
     [{:keys [status headers body error]}]
      (if error
        (do (println  " -> " error) {})
        (parse-json body)
      ))


(defn watcher
  "Watcher function for agents, print agent's new state"
  [k ag old new]
  (println k "->" new ) (flush))

