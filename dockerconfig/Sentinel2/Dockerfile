FROM continuumio/miniconda3:4.7.12

LABEL maintainer="Luigi Di Fraia"

RUN conda update conda --quiet --yes --freeze-installed \
    && conda clean --all -f -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN conda install --quiet --yes --freeze-installed \
    boto3 \
    geopandas \
    pyyaml \
    rasterio \
    && conda clean --all -f -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN pip install --no-cache-dir \
    google-cloud-storage \
    asynchronousfilereader \
    redis \
    sentinelsat

RUN wget --quiet http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run && \
    /bin/sh ./Sen2Cor-02.08.00-Linux64.run && \
    rm ./Sen2Cor-02.08.00-Linux64.run

#RUN apt-get install -y --no-install-recommends xmlstarlet && \
#    rm -rf /var/lib/apt/lists/*

#RUN xmlstarlet edit -L -u "//Downsample_20_to_60" -v "FALSE" $HOME/sen2cor/2.8/cfg/L2A_GIPP.xml

# ------------------------------------------

RUN conda install --quiet --yes --freeze-installed \
    jupyterlab \
    notebook \
    && conda clean --all -f -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN pip install jupyter -U && pip install jupyterlab

ENTRYPOINT ["jupyter", "lab","--ip=0.0.0.0","--port=8888","--allow-root","--NotebookApp.token=pwd"]