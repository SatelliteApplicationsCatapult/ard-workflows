# ARD workflow deployment with Kubernetes

## Redis master server deployment
In order to deploy the master issue the following:
```helm install stable/redis --namespace redis --name redis --values 04-config-redis.yaml```

### Redis master server testing
To sanity check the master server issue the following: 
```
$ kubectl run --namespace redis redis-client --rm --tty -i --restart='Never' \
            --image docker.io/bitnami/redis:5.0.5-debian-9-r104 -- bash

I have no name!@redis-client:/$ redis-cli -h redis-master
redis-master:6379> rpush job2 "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE"
(integer) 1
redis-master:6379> lrange job2 0 -1
1) "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE"
redis-master:6379>
```
