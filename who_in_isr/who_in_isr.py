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
1 in isr: 100% (100/100)
2 in isr: 100% (100/100)
4 in isr: 35%  (7/20)
5 in isr: 99%  (99/100)

"""

results = {}

js = json.load(sys.stdin)

# add all brokers to the result dict
all_broker_ids = [ broker_info['id'] for broker_info in js['brokers']]
for broker_id in all_broker_ids:
    results[broker_id] = {}
    for replica_id in all_broker_ids:
        results[broker_id][replica_id] = { "in_replica" : 0,
                                           "in_isr" : 0 }

for topic in js['topics']:
    for partition_js in topic['partitions']:
        replicas = set([ r['id'] for r in partition_js['replicas'] ])
        isrs = set([ i['id'] for i in partition_js['isrs'] ])
        leader = partition_js['leader']

        # offline partitions show up with leader == -1
        if leader == -1 and -1 not in results:
            results[-1] = {}
            for replica_id in all_broker_ids:
                results[-1][replica_id] = { "in_replica" : 0,
                                            "in_isr" : 0 }

        for replica in replicas:
            results[leader][replica]["in_replica"] += 1
            if replica in isrs:
                results[leader][replica]["in_isr"] += 1


        
for (leader, all_followers) in results.items():
    print "Broker %d" % leader
    for (follower, follower_data) in all_followers.items():
        percent = 0
        if follower_data["in_replica"] > 0:
            percent = float(follower_data["in_isr"])/follower_data["in_replica"] * 100
            
        print "  %d in isr: %8.2f%% %6s/%s" % (
            follower,
            percent,
            "(%d" % follower_data["in_isr"], "%d)" % follower_data["in_replica"])
    print
