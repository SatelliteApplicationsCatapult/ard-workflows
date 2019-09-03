#!/usr/bin/env python

################
# ARD workflow #
################

import json
from utils.prepS2 import prepareS2

def process_scene(json_data):
    loaded_json = json.loads(json_data)
    #prepareS2(loaded_json["in_scene"], loaded_json["out_dir"], inter_dir=loaded_json["inter_dir"], prodlevel=loaded_json["prodlevel"], source=loaded_json["source"])
    prepareS2(loaded_json["in_scene"], inter_dir=loaded_json["inter_dir"])

##################
# Job processing #
##################

import rediswq

host="redis-master"
# Uncomment next two lines if you do not have Kube-DNS working.
# import os
# host = os.getenv("REDIS_SERVICE_HOST")

q = rediswq.RedisWQ(name="jobS2", host=host)
print("Worker with sessionID: " +  q.sessionID())
print("Initial queue state: empty=" + str(q.empty()))
while not q.empty():
  item = q.lease(lease_secs=1800, block=True, timeout=600) 
  if item is not None:
    itemstr = item.decode("utf=8")
    print("Working on " + itemstr)
    #time.sleep(10) # Put your actual work here instead of sleep.
    process_scene(itemstr)
    q.complete(item)
  else:
    print("Waiting for work")
print("Queue empty, exiting")

