# ARD workflow container infrastructure

## Base image
The provided [Dockerfile](Dockerfile) creates a Docker image with an ARD workflow set up by means of Miniconda v4.7.10.
[Jupyter Notebook](https://jupyter.org/) is included as well and started once the Docker image is run.

## Docker Compose
A [Docker Compose](docker-compose.yml) example file is provided to set up a fully functional ARD workflow instance.\
To use it you can issue:

```docker-compose up```

Once the above completes the job queue is ready to be filled in with scene names by issuing:

```
docker exec -it redis /bin/bash
redis-cli -h redis
rpush job2 "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE"
...
lrange job2 0 -1
```

At any time afterwards, the queue can be processed interactively by running the [worker](worker.ipynb) Jupyter Notebook.

## Environment variables for Docker Compose
Environment variables can be set in a `.env` file for Docker Compose. You might use [.env.example](./.env.example) as a starting point.

## AWS access
In order to be able to get/put data from/to S3, you need to ensure that the environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` are set.

## Jupyter Notebook
Jupyter Notebook can be accessed at the URL: http://{Serve's IP Address}:8888\
For the access token, check the CMD statement within the [Dockerfile](Dockerfile).
