FROM continuumio/miniconda3:4.7.10

LABEL maintainer="Luigi Di Fraia"

RUN conda install --quiet --yes \
    boto3 \
    geopandas \
    pyyaml \
    rasterio \
    redis \
    && conda clean --all -f -y

RUN pip install --no-cache-dir \
    google-cloud-storage \
    sentinelsat

RUN wget --quiet http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run && \
    /bin/sh ./Sen2Cor-02.08.00-Linux64.run && \
    rm ./Sen2Cor-02.08.00-Linux64.run

#CMD [ "/bin/bash" ]

RUN conda install --quiet --yes \
    jupyter \
    && conda clean --all -f -y && \
    mkdir /opt/notebooks

COPY utils /opt/notebooks/utils

COPY dev_process_sentinel2.ipynb /opt/notebooks

COPY aws_creds.csv /opt/notebooks

COPY rediswq.py /opt/notebooks

COPY worker.ipynb /opt/notebooks

CMD jupyter notebook \
    --allow-root \
    --notebook-dir=/opt/notebooks \
    --NotebookApp.ip='0.0.0.0' \
    --NotebookApp.port='8888' \
    --NotebookApp.token='secretpassword' \
    --NotebookApp.open_browser='False'
