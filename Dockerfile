FROM continuumio/miniconda3:4.7.12

LABEL maintainer="Luigi Di Fraia"

RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

RUN conda update conda --quiet --yes --freeze-installed \
    && conda clean --all -f -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN conda install --quiet --yes --freeze-installed \
    boto3 \
    geopandas \
    hdmedians \
    matplotlib \
    pandas \
    pyyaml \
    rasterio \
    requests \
    scikit-learn \
    xarray \
    && conda clean --all -f -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN pip install --no-cache-dir \
    asynchronousfilereader \
    redis \
	salem \
	sklearn-xarray \
	rioxarray \
	boto3

# ------------------------------------------

RUN conda install --quiet --yes --freeze-installed \
    jupyterlab \
    notebook \
    && conda clean --all -f -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN pip install jupyter -U && pip install jupyterlab

EXPOSE 8888

ENTRYPOINT ["jupyter", "lab","--ip=0.0.0.0","--port=8888","--allow-root","--NotebookApp.token=pwd"]
