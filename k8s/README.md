# ARD workflow deployment with Kubernetes and Helm

## Architecture

We run a Kubernetes `Job` with multiple parallel worker processes in a given pod.\
As each pod is created, it picks up one unit of work from a task queue, processes it, and repeats until the end of the queue is reached.\
We use Redis as storage service to hold the work queue and store our work items. Each work item represents one scene to be processed through an ARD workflow.

## Redis master server deployment

In order to deploy the master issue the following:
```bash
RELEASEREDIS=redis
NAMESPACE=ard

helm upgrade --install $RELEASEREDIS stable/redis \
  --namespace $NAMESPACE \
  --version=9.1.3 \
  --values 04-config-redis.yaml
```

### Redis master server testing

To sanity check the master server issue the following: 
```bash
$ kubectl run --namespace $NAMESPACE redis-client --rm --tty -i --restart='Never' \
  --image docker.io/bitnami/redis:5.0.5-debian-9-r104 -- bash

I have no name!@redis-client:/$ redis-cli -h redis-master

redis-master:6379>
```

### Redis job definitions

The list with key `jobS2` is our work queue. Add jobs with e.g.:
```bash
$ kubectl run --namespace $NAMESPACE redis-client --rm --tty -i --restart='Never' \
  --image docker.io/bitnami/redis:5.0.5-debian-9-r104 -- bash

I have no name!@redis-client:/$ redis-cli -h redis-master

redis-master:6379> rpush jobS2 '{"in_scene": "S2A_MSIL2A_20190812T235741_N0213_R030_T56LRR_20190813T014708", "inter_dir": "/data/intermediate/"}'
(integer) 1
redis-master:6379> lrange jobS2 0 -1
1) '{"in_scene": "S2A_MSIL2A_20190812T235741_N0213_R030_T56LRR_20190813T014708", "inter_dir": "/data/intermediate/"}'
```

## Job processor deployment

Instantiate job processors with:
```bash
RELEASEARD=s2job

git clone https://github.com/SatelliteApplicationsCatapult/helm-charts.git

helm upgrade --install $RELEASEARD ./helm-charts/stable/ard-workflow-s2 \
  --namespace $NAMESPACE
```

## Cleaning up

:warning: Dangerous Zone :warning:

If you wish to undo changes to your Kubernetes cluster, simply issue the following commands.

```bash
helm delete $RELEASEREDIS --purge
helm delete $RELEASEARD --purge
kubectl delete namespace $NAMESPACE
```
