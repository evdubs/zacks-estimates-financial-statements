#lang racket/base

(require gregor
         gregor/period
         gregor/time
         net/url
         racket/cmdline
         racket/file
         racket/list
         racket/port
         tasks
         threading)

(define (download-day date)
  (make-directory* (string-append "/var/tmp/zacks/earnings-calendar/" (~t (today) "yyyy-MM-dd")))
  (call-with-output-file (string-append "/var/tmp/zacks/earnings-calendar/" (~t (today) "yyyy-MM-dd") "/"
                                        (~t date "yyyy-MM-dd") ".json")
    (λ (out)
      (~> (string-append "https://www.zacks.com/includes/classes/z2_class_calendarfunctions_data.php?calltype=eventscal&date="
                         (number->string (->posix (at-time date (time 6)))))
          (string->url _)
          (get-pure-port _)
          (copy-port _ out)))
    #:exists 'append))

(define end-date (make-parameter (+days (today) (* 7 6))))

(define start-date (make-parameter (today)))

(command-line
 #:program "racket earnings-calendar-extract.rkt"
 #:once-each
 [("-e" "--end-date") ed
                      "End date. Defaults to today + 6 weeks"
                      (end-date (iso8601->date ed))]
 [("-s" "--start-date") sd
                        "Start date. Defaults to today"
                        (start-date (iso8601->date sd))])

(define delay-interval 10)

(with-task-server (for-each (λ (i) (schedule-delayed-task (λ () (download-day (+days (start-date) i)))
                                                          (* i 10)))
                            (range 0 (period-ref (period-between (start-date) (end-date) '(days)) 'days)))
  ; add a final task that will halt the task server
  (schedule-delayed-task
   (λ () (schedule-stop-task)) (* delay-interval (period-ref (period-between (start-date) (end-date) '(days)) 'days)))
  (run-tasks))
