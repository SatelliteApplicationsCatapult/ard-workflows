# A super cool project name
> A really useful summary description.


Catapult hosts a variety of satellite Earth Observation (EO) datasets that it's expected would have far more use due to improved **discoverability**, **searchability** and **accessibility** if they were...
1. hosted on publicly accessible object storage
2. formatted to be cloud-optimised
3. consistently catalogued 

Here we do some exploratory work in using Catapult's internal **[object storage](https://en.wikipedia.org/wiki/Object_storage)**, Cloud Optimised Geotiffs (**[COGs](https://www.cogeo.org/)**) and SpatioTemporal Asset Catalogs (**[STAC](https://stacspec.org/)**) to address each of these components for a selection of Catapult-hosted and publicly available EO datasets.

The initial exploration covers a variety of very high resolution images procured from commercial providers with flexible license agreements for [pubic-sector use cases](https://www.gov.uk/government/news/free-satellite-data-available-to-help-tackle-public-sector-challenges) and a nascent flavour of Analysis Reaady Data (**[ARD](http://ceos.org/ard/)**) for [NovaSAR](https://sa.catapult.org.uk/facilities/novasar-1/). 





## Install

`pip install sac_stac` (package not created yet)

## Dev

Alongside trying out some stac pieces this is also a first attempt at using [nbdev](https://nbdev.fast.ai/). (Inspired by the not-so-controversial "I like notebooks" [presentation](https://www.youtube.com/watch?v=9Q6sLbz37gk&feature=youtu.be).) The setup for development is simply based around a Docker container serving a Jupyter Lab environment. Folding package requirements within settings.ini is the next step for making use of...). (Inspired by the not-so-controversial "[I like notebooks presentation](https://www.youtube.com/watch?v=9Q6sLbz37gk&feature=youtu.be)".) The setup for development is simply based around a Docker container serving a Jupyter Lab environment. Folding package requirements within settings.ini is the next step for making use of...

Will want to change volume mount of data directory within docker-compose.

## So far...

There are some basic utilities for working with the sedas api, converting rasters to COG format and dealing with S3-like storage.

```python
get_sedas_collections()
```




    'Available groups are: Cosmo-SkyMed, SPOT, Pleiades, S1, S2, AIRSAR'



And these are built up to be used across dataset specific characteristics. Basically addressing different dir structures, mosaic and band stacking requirements, etc. For example with Pleiades...

## There soon will be...
