(ns ma_perf_urls.core
  (:use clojure.java.io)
  (:use clj-ssh.ssh)
  (:use clj-ssh.ssh)
  (:use clj-time.core)
  (:use clj-time.format)
  (:use clj-time.local))

(def hosts (list "add server here"))

(def user "jvmuser")

(def date-matcher #"^(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}),\d+ (.*)")

(def request-matcher #"c\.g\.m\.routers\.EndpointUrls\$ - request (?:/mobile-aggregator/public)?(\S+)")

(def log-path "/executable-jar-apps/mobile-aggregator/logs/mobile-aggregator.log")

(def log-date-formatter (formatter "yyyy-MM-dd HH:mm:ss"))

(def requests-duration (minutes 30))

(defn log-filehandles [hosts]
  (let [f (fn [key host]
            (let [agent (ssh-agent {})
                  local-filename (concat "/tmp/ma_perf_urls_" (.toString key) ".log")]
              (let [session (session agent host {:strict-host-key-checking :no
                                                 :username user})]
                (with-connection session
                  (let [channel (ssh-sftp session)]
                    (with-channel-connection channel
                      (sftp channel {} :get log-path local-filename)
                      (reader local-filename)))))))]
        (map-indexed f hosts)))

(defn str< [str1 str2]
  (= (compare str1 str2) -1))

(defn request-from-line [line]
  (let [time line
        match (re-find request-matcher line)]
    (if (nil? match)
      nil
      (nth match 1))))

(defn requests-from-lines [lines]
  (mapcat #(let [time (first %1)
                 request (request-from-line (rest %1))]
             (if (nil? request)
               []
               [[time request]]))
          lines))

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
  (Integer. (concat (re-seq #("\\d+") str))))

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
