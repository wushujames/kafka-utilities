# kafka-utilities

# who_in_isr
Requires kafkacat, and in particular, a version that supports json output (-J)
```
$ kafkacat -L -b broker.example.com  -J | who_in_isr.py
Broker 1
  1 in isr:   100.00%
  2 in isr:   100.00%
  3 in isr:   100.00%
  4 in isr:     0.00%
  5 in isr:   100.00%

Broker 2
  1 in isr:   100.00%
  2 in isr:   100.00%
  3 in isr:   100.00%
  4 in isr:    10.87%
  5 in isr:   100.00%

Broker 3
  1 in isr:   100.00%
  2 in isr:   100.00%
  3 in isr:   100.00%
  4 in isr:     0.00%
  5 in isr:   100.00%

Broker 4
  4 in isr:   100.00%

Broker 5
  1 in isr:   100.00%
  2 in isr:   100.00%
  3 in isr:   100.00%
  4 in isr:     0.00%
  5 in isr:   100.00%

Broker -1
  1 in isr:     0.00%
  4 in isr:     0.00%
  5 in isr:     0.00%

```
How to read this:
* For partitions with leaders on broker 1, broker 4 is missing from the ISR for all the replicas it is supposed to be part of.
* For partitions with leaders on broker 2, broker 4 is only in 10.87% of the replicas it is supposed to be part of.

Looking at all brokers, it is always broker 4 that is missing from ISRs. This indicates that it is likely that broker 4 is the one having problems.

Broker -1 means that there are some partitions have no leaders. They are likely offline.
