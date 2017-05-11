;;; to use this project, put the following where quickload can find them:
;;; git clone https://github.com/ivan4th/i4-diet-utils.git
;;; git clone https://github.com/ivan4th/cl-mqtt.git

;;; To control the simulator, publish on the in topic, like
;;; mosquitto_pub -t sensationIII/cmd -h test.mosquitto.org -m "{\"line\":2}"
;;; This would reset the simulator to read from line 2 of the data file


;;(in-package :sensationIII-simulator)
;; TODO: write a defpackage and a defsystem that :uses and :depends-on these:
(ql:quickload :cl-mqtt)
(ql:quickload :cl-async)
(ql:quickload :cl-ppcre)

;; Util macros
(defmacro when-it (test &body then)
  (let ((it (intern (string 'it))))
    `(let ((,it ,test))
       (when ,it ,@then))))

(defmacro if-it (test then &optional else)
  (let ((it (intern (string 'it))))
    `(let ((,it ,test))
       (if ,it ,then ,else))))

(defun mqtt-read (msg)
  "Decode the stuff that mqtt:subscribe gives us on message"
  (babel:octets-to-string
    (mqtt:mqtt-message-payload msg)
    :encoding :utf-8))

(defun csv-string->list-of-strings (string)
  (mapcar #'(lambda (str) (string-trim '(#\Space) str))
          (cl-ppcre:split "," (string-right-trim '(#\Return) string))))

(defun csv-string->values (string)
  "Keep the first value as it is, use lisp reader on the rest.
   You must really trust that the strings don't contain malicious lisp code when you use this function..."
  (let ((strings (csv-string->list-of-strings string)))
    (cons (first strings) (mapcar #'read-from-string (rest strings)))))

(defun lists->json (keys values)
  (cl-json:encode-json-to-string (mapcar #'cons keys values)))

(defun find-key (key json-string)
  (assoc key (cl-json:decode-json-from-string json-string)))

(defun sensationIII-simulator (filename broker &key
                                                 (delay-seconds 1)
                                                 (port 1883)
                                                 (topic-out "sensationIII/toungedata")
                                                 (topic-in "sensationIII/cmd"))
  "Publish data from adjustable file pointer to MQTT broker at a configurable pace.
   File pointer and pace are controlled through topic-in via broker, using the keys
   * line (sets the line in the file you want to read from)
   * position (sets byte position you want to read from, rounded up to the next newline)
   * delay-seconds (sets the seconds you want to pass between each send)
   "
  (format t "Starting MQTT connected sensor simulator~%")
  (with-open-file (file-stream filename)
    (defun pop-line ()
      "Pop a line from the file, reset file pointer if necessary"
      (if-it (read-line file-stream nil nil)
             it
             (progn (file-position file-stream 0)
                    (read-line file-stream nil nil)
                    (read-line file-stream nil nil))))

    (defun handle-mqtt-cmd (message)
      "Set delay between messages and file position according to received MQTT message."
      (let ((decoded-message (mqtt-read message)))
        (when-it (find-key :delay-seconds decoded-message)
          (setf delay-seconds (cdr it)))
        (when-it (find-key :position decoded-message)
          (file-position file-stream (cdr it))
          (pop-line))
        (when-it (find-key :line decoded-message)
          (file-position file-stream 0)
          (dotimes (i (- (cdr it) 1))
            (pop-line)))))

    (defun periodically-publish (mqtt-connection keys)
      "When called within an event loop publishes according to delay-seconds and file position."
      (let (event)
        (labels ((main ()
                   (mqtt:publish mqtt-connection topic-out
                                 (lists->json keys (csv-string->values (pop-line))))
                   (when event
                     (setf event (as:delay #'main :time delay-seconds)))))
          (setf event (as:delay #'main :time delay-seconds))
          (lambda ()
            (when event
              (as:remove-event event)
              (setf event nil))))))

    ;; Actual simulation starts here
    (as:with-event-loop (:catch-app-errors t)
      (bb:alet ((keys (csv-string->list-of-strings (read-line file-stream nil)))
                (mqtt-connection (mqtt:connect broker :port port :on-message #'handle-mqtt-cmd)))
        (mqtt:subscribe mqtt-connection topic-in)
        (periodically-publish mqtt-connection keys)))))
