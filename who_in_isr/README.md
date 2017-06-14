# who_in_isr
When I have under replicated partitions in my kafka cluster, this handy utility lets me know if a particular broker is at fault.

Requires kafkacat, and in particular, a version that supports json output (-J)
```
$ kafkacat -L -b broker.example.com  -J | who_in_isr.py
Broker 1
  1 in isr:   100.00% (100/100)
  2 in isr:   100.00% (100/100)
  3 in isr:   100.00% (235/235)
  4 in isr:     0.00% (0/120)
  5 in isr:   100.00% (100/100)

Broker 2
  1 in isr:   100.00% (100/100)
  2 in isr:   100.00% (200/200)
  3 in isr:   100.00% (100/100)
  4 in isr:    12.50% (25/200)
  5 in isr:   100.00% (100/100)

Broker 3
  1 in isr:   100.00% (100/100)
  2 in isr:   100.00% (200/200)
  3 in isr:   100.00% (300/300)
  4 in isr:     0.00% (0/100)
  5 in isr:   100.00% (100/100)

Broker 4
  1 in isr:     0.00% (0/100)
  2 in isr:     0.00% (0/100)
  3 in isr:     0.00% (0/100)
  4 in isr:   100.00% (100/100)
  5 in isr:     0.00% (0/100)

Broker 5
  1 in isr:   100.00% (100/100)
  2 in isr:   100.00% (100/100)
  3 in isr:   100.00% (100/100)
  4 in isr:     0.00% (0/100)
  5 in isr:   100.00% (100/100)

Broker -1
  1 in isr:     0.00% (0/20)
  2 in isr:     0.00% (0/20)
  3 in isr:     0.00% (0/20)
  4 in isr:     0.00% (0/30)
  5 in isr:     0.00% (0/50)
```

How to read this:
* For partitions with leaders on broker 1, broker 4 is missing from the ISR for all the replicas it is supposed to be part of.
* For partitions with leaders on broker 2, broker 4 is only in the ISR for 12.50% of the replicas it is supposed to be part of.
* The numbers in parenthesis are the numbers that are used the calculate the percentage. The denominator is the number of partitions for which the broker is supposed to be in the ISR. The numerator is the number of partitions for which the broker is currently in the ISR.

Looking at all brokers, it is always broker 4 that is missing from ISRs. This indicates that it is likely that broker 4 is the one having problems.

Broker -1 means that there are some partitions have no leaders. They are likely offline.
