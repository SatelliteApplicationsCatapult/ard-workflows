# ARD workflow container infrastructure

## Base image
The provided [Dockerfile](Dockerfile) creates a Docker image with an ARD workflow set up by means of Miniconda v4.7.10.
[Jupyter Notebook](https://jupyter.org/) is included as well and started once the Docker image is run.

## Docker Compose
A [Docker Compose](docker-compose.yml) example file is provided to set up a fully functional ARD workflow instance.\
To use it you can issue, for example for 3 worker containers:

```docker-compose up --scale jupyter-worker=3 -d```

Once the above completes, the job queue is ready to be filled in with scene names by issuing:

```
docker exec -it redis /bin/bash
redis-cli -h redis
rpush jobS2 '{"in_scene": "S2A_MSIL2A_20190812T235741_N0213_R030_T56LRR_20190813T014708", "inter_dir": "/data/intermediate/"}'
...
lrange jobS2 0 -1
```

At any time afterwards, the queue can be processed interactively by running the [worker](worker.ipynb) Jupyter Notebook.

## Environment variables for Docker Compose
Environment variables can be set in a `.env` file for Docker Compose. You might use [.env.example](./.env.example) as a starting point.

## AWS access
In order to be able to get/put data from/to S3, you need to ensure that the environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` are set.

## Jupyter Notebook
Jupyter Notebook can be accessed at the URL: http://{Serve's IP Address}:8888 for the first replica, 8889 for the second one if present, and so on.\
For the access token, check the CMD statement within the [Dockerfile](Dockerfile).

## TODO
The `process_scene()` method will be generalised for multiple satellites and source locations, thus taking multiple arguments.\
Consequently, jobs will be defined by means of JSON entries, such as:

```
'{"in_scene": "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE", "s3_bucket": "public-eo-data", "s3_dir": "fiji/Sentinel_2_test/", "inter_dir": "/data/intermediate", "prodlevel": "L2A", "source": "gcloud"}'
```
