(ns ma_perf_urls.core
  (:use clj-ssh.ssh)
  (:use clj-time.core)
  (:use clj-time.format)
  (:use clj-time.local))

(def hosts (list "put hosts here"))

(def pk "/home/robert/.ssh/id_rsa")

(def user "jvmuser")

(def date-matcher #"^(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}),\d+ (.*)")

(def request-matcher #"c\.g\.m\.routers\.EndpointUrls\$ - request (?:/mobile-aggregator/public)?(\S+)")

(def log-path "/executable-jar-apps/mobile-aggregator/logs/mobile-aggregator.log")

(def log-date-formatter (formatter "yyyy-MM-dd HH:mm:ss"))

(def requests-duration (minutes 30))

(defn log-filehandles [hosts]
  (let [f (fn [host]
            (let [agent (ssh-agent {})]
              (add-identity agent {:private-key-path pk})
              (let [session (session agent host {:strict-host-key-checking :no
                                                 :username user})]
                (with-connection session
                  ((ssh session {:in (str "cat " log-path)}) :out)))))]
        (map f hosts)))

(defn str< [str1 str2]
  (= (compare str1 str2) -1))

(defn request-from-line [line]
  (let [time line
        match (re-find request-matcher line)]
    (if (nil? match)
      nil
      (nth match 1))))

(defn requests-from-lines [lines]
  (mapcat #(let [[time rest] %1
                 request (request-from-line rest)]
             (if (nil? request)
               []
               [[time request]]))))

(defn entries-since [log-lines time]
  (let [time-string (unparse log-date-formatter time)]
    (drop-while #(let [[line-time-string _] %1]
                   (str< line-time-string time-string)))))

(defn split-lines-on-date [lines]
  (map rest (re-seq date-matcher lines)))

(defn start-time []
  (minus (local-now) requests-duration))

(defn requests-from-log [log start]
  (let [lines (entries-since (split-lines-on-date (line-seq log)) start)]
    (requests-from-lines lines)))

(defn string->timestamp [str]
  (Integer. (concat re-seq #("\\d+"))))

(defn flatten-requests [requests]
  (lazy-seq
   (if (nil? requests)
     nil
     (let [sorted-reqs (sort-by #(string->timestamp (first %1)) requests)
           front-reqs (first sorted-reqs)
           next-req (first front-reqs)
           front-reqs-rest (rest front-reqs)]
       (cons next-req
             (flatten-requests
              (if (nil? front-reqs-rest)
                (rest sorted-reqs)
                (cons front-reqs-rest (rest sorted-reqs)))))))))

(defn -main []
  (let [filehandles (log-filehandles hosts)
        start (start-time)
        requests (flatten-requests (map #(requests-from-log %1 start) filehandles))]
    (doseq [request requests]
      (let [[time url] request]
        (println url)))))
