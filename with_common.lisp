; cd .roswell/local-projects
; git clone https://github.com/ivan4th/i4-diet-utils.git
; git clone https://github.com/ivan4th/cl-mqtt.git

(in-package :sensationIII-simulator)

(ql:quickload :cl-mqtt)
(ql:quickload :cl-async)
(ql:quickload :cl-ppcre)

(defun mqtt-read (msg)
  (babel:octets-to-string
    (mqtt:mqtt-message-payload msg)
    :encoding :utf-8))


(defun test-it (host port)
  (bb:alet ((conn (mqtt:connect
                    host
                    :port port
                    :on-message #'(lambda (message)
                                    (format t "~%RECEIVED: ~s~%"
                                            (mqtt-read message))))))
           (bb:walk
             (mqtt:subscribe conn "/a/#")
             (mqtt:subscribe conn "/b/#")
             (mqtt:publish conn "/a/b" "whatever1")
             (mqtt:unsubscribe conn "/a/#")
             (mqtt:publish conn "/a/b" "whatever2")
             (mqtt:publish conn "/b/c" "foobar")
             (as:with-delay (1)
                            (mqtt:disconnect conn))))
  (values))

;; Open the filestream in
;; Enter a send loop
;; Send a line
;; Try to receive a message
;; If we got a message on sensationIII/cmd/freq:
;;   Set (file-position in (mqtt->file-position mqtt-message))
;; If we got a message on sensationIII/cmd/point:
;;   Set (file-position in (mqtt->file-position mqtt-message))
;; On every loop
;; (as:with-delay ((/ 1 (mqtt->freq mqtt-message)))
;;                (mqtt:publish *mqtt-connection*
;;                              (data-line->json (read-line in))))

(defmacro when-it (test &body then)
  (let ((it (intern (string 'it))))
    `(let ((,it ,test))
       (when ,it ,@then))))

(defun csv-string->list-of-strings (string)
  (mapcar #'(lambda (str) (string-trim '(#\Space) str))
          (cl-ppcre:split "," (string-right-trim '(#\Return) string))))

(defun csv-string->values (string)
  (let ((strings (csv-string->list-of-strings string)))
    (cons (first strings) (mapcar #'read-from-string (rest strings)))))

(defun lists->json (keys values)
  (cl-json:encode-json-to-string (mapcar #'cons keys values)))

(defun find-key (key json-string)
  (assoc key (cl-json:decode-json-from-string json-string)))

(defun sensationIII-simulator (filename broker &key
                                                 (delay-seconds 10)
                                                 (port 1883)
                                                 (topic-out "sensationIII/toungedata")
                                                 (topic-in "sensationIII/cmd"))
  "Publish data from adjustable file pointer to MQTT broker at a configurable pace."
  (with-open-file (file-stream filename)
    (defun handle-mqtt-cmd (message)
      "Set delay between messages and file position according to received MQTT message."
      (let ((decoded-message (mqtt-read message)))
        (when-it (find-key :delay-seconds decoded-message)
          (setf delay-seconds (cdr it)))
        (when-it (find-key :position decoded-message)
          (file-position file-stream (cdr it))
          (read-line file-stream nil))))
    (block simulator 
      (bb:alet ((keys (csv-string->list-of-strings (read-line file-stream nil)))
                (mqtt-connection (mqtt:connect broker :port port :on-message #'handle-mqtt-cmd)))
        (as:with-event-loop ;; Do MQTT job in asynchronus event loop
            (bb:finally
                (bb:walk
                  (mqtt:subscribe mqtt-connection topic-in)
                  (as:with-interval (delay-seconds)
                    (let ((line (read-line file-stream nil)))
                      (if line
                          (progn 
                            (mqtt:publish mqtt-connection topic-out
                                          (lists->json keys (csv-string->values line)))
                            (format t "published~%"))
                          (progn
                            (format t "reached end of data file~%"))
                          (return-from simulator t))))))
          (mqtt:disconnect mqtt-connection)
          )))))))

(as:start-event-loop #'(lambda () (test-it "test.mosquitto.org" 1883)))
