#!/usr/bin/python

import sys
import json

"""
Usage:
$ kafkacat -L -b broker.example.com  -J | python who_in_isr.py
"""
"""
desired output:
broker 3
1 in isr: 100%
2 in isr: 100%
4 in isr: 35%
5 in isr: 99%

"""

results = {}

js = json.load(sys.stdin)
for topic in js['topics']:
    for partition_js in topic['partitions']:
        replicas = set([ r['id'] for r in partition_js['replicas'] ])
        isrs = set([ i['id'] for i in partition_js['isrs'] ])
        leader = partition_js['leader']

        if leader not in results:
            results[leader] = {}

        for replica in replicas:
            if replica not in results[leader]:
                results[leader][replica] = { "in_replica" : 0,
                                             "in_isr" : 0 }
            results[leader][replica]["in_replica"] += 1
            if replica in isrs:
                results[leader][replica]["in_isr"] += 1


        
for (leader, all_followers) in results.items():
    print "Broker %d" % leader
    for (follower, follower_data) in all_followers.items():
        print "  %d in isr: %8.2f%%" % (
            follower, float(follower_data["in_isr"])/follower_data["in_replica"] * 100)
    print
