# ARD workflow deployment with Kubernetes

## Architecture
We run a Kubernetes `Job` with multiple parallel worker processes in a given pod.\
As each pod is created, it picks up one unit of work from a task queue, processes it, and repeats until the end of the queue is reached.\
We use Redis as storage service to hold the work queue and store our work items. Each work item represents one scene to be processed through an ARD workflow.

## Redis master server deployment
In order to deploy the master issue the following:
```
RELEASEREDIS=redis
NAMESPACEREDIS=redis

helm upgrade --install $RELEASEREDIS stable/redis \
  --namespace $NAMESPACEREDIS \
  --version=9.1.3 \
  --values 04-config-redis.yaml
```

### Redis master server testing
To sanity check the master server issue the following: 
```
$ kubectl run --namespace $NAMESPACEREDIS redis-client --rm --tty -i --restart='Never' \
  --image docker.io/bitnami/redis:5.0.5-debian-9-r104 -- bash

I have no name!@redis-client:/$ redis-cli -h redis-master
```
The list with key `job2` will be our work queue:
```
redis-master:6379> rpush job2 "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE"
(integer) 1
redis-master:6379> lrange job2 0 -1
1) "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE"
redis-master:6379>
```

## Running jobs
Start a job with:
```
kubectl apply -f 06-job.yaml
```
