import yaml
import glob
import rasterio
from rasterio.crs import CRS
# from rasterio.transform import Affine
from affine import Affine
import rioxarray
import xarray as xr
from matplotlib import pyplot as plt
from datetime import datetime
from subprocess import Popen, PIPE, STDOUT
import pandas as pd
import os
import numpy as np
import shutil
import logging
import logging.handlers
from dateutil.parser import parse
import uuid
import geopandas as gpd
import rasterio
import rasterio.features
import gdal
import gc
import traceback
import requests
import rioxarray as rxr

# ml stuff
from sklearn_xarray import wrap
from sklearn.ensemble import RandomForestClassifier

from . prep_utils import *
from . dc_import_export import export_xarray_to_geotiff



def stream_yml(s3_bucket, s3_path):
    return yaml.safe_load(requests.get(f"{os.getenv('AWS_S3_ENDPOINT_URL')}/{s3_bucket}/{s3_path}").text)

def get_ref_channel(prod):
    if ('LANDSAT' in prod) | ('SENTINEL_2' in prod): return 'swir1'
    elif 'SENTINEL_1' in prod: return 'vv'
    
def get_qa_channel(prod):
    if 'LANDSAT' in prod: return 'pixel_qa'
    elif 'SENTINEL_2' in prod: return 'scene_classification'
    elif 'SENTINEL_1' in prod: return 'layovershadow_mask'

def get_remote_band_paths(s3_bucket, yml_paths, band_nms):
    paths, bands = [], []
    for yml_path in yml_paths:
        yml = stream_yml(s3_bucket, yml_path)
        for band_nm in band_nms:
            try:
                paths.append(f"{os.getenv('AWS_S3_ENDPOINT_URL')}/{s3_bucket}/{'/'.join(yml_path.split('/')[:-1])}/{yml['image']['bands'][band_nm]['path']}")
                bands.append(band_nm)
            except:
                pass
    return paths, bands

def load_bands(band_paths, lvl=None):
    dask_chunks = dict(x = 2000, y = 2000)
    return [rxr.open_rasterio(band, chunks=dask_chunks, masked=True, sharing=True) for band in band_paths]

def rename_bands(in_xr, des_bands, position):
    in_xr.name = des_bands[position]
    return in_xr

def load_img(bands_data, band_nms):
    """ assumes first band is ref"""
    atts = bands_data[0].attrs
    bands_data = [ rename_bands(band_data, band_nms, i) for i,band_data in enumerate(bands_data) ] # rename
    bands_data = [ bands_data[i].rio.reproject_match(bands_data[0]) for i in range(len(band_nms)) ] # repro+resample+extent
    bands_data = [ xr.align(bands_data[0], bands_data[i], join="override")[1] for i in range(len(band_nms)) ] # force align
    bands_data = xr.merge(bands_data).rename({'band': 'time'}).isel(time = 0).drop(['time']) # ensure band names & dims consistent
    bands_data = bands_data.assign_attrs(atts)
    return bands_data

def get_valid(ds, prod):
    # Identify pixels with valid data
    if 'LANDSAT_8' in prod:
        good_quality = (
            (ds.pixel_qa == 322)  | # clear
            (ds.pixel_qa == 386)  |
            (ds.pixel_qa == 834)  |
            (ds.pixel_qa == 898)  |
            (ds.pixel_qa == 1346) |
            (ds.pixel_qa == 324)  | # water
            (ds.pixel_qa == 388)  |
            (ds.pixel_qa == 836)  |
            (ds.pixel_qa == 900)  |
            (ds.pixel_qa == 1348)
        )
    elif prod in ["LANDSAT_7", "LANDSAT_5", "LANDSAT_4"]:    
        good_quality = (
            (ds.pixel_qa == 66)   | # clear
            (ds.pixel_qa == 130)  |
            (ds.pixel_qa == 68)   | # water
            (ds.pixel_qa == 132)  
        )
    elif 'SENTINEL_2' in prod:
        good_quality = (
            (ds.scene_classification == 2) | # mask in DARK_AREA_PIXELS
#             (ds.scene_classification == 3) | # mask in CLOUD_SHADOWS
            (ds.scene_classification == 4) | # mask in VEGETATION
            (ds.scene_classification == 5) | # mask in NOT_VEGETATED
            (ds.scene_classification == 6) | # mask in WATER
            (ds.scene_classification == 7)   # mask in UNCLASSIFIED
        )
    elif 'WOFS_SUMMARY' in prod:
        good_quality = (
            (ds.pc >= 0)
        )
    elif 'SENTINEL_1' in prod:
        good_quality = (
            (ds.vv != 0)
        )
    return good_quality

def band_name_water(prod_path):
    """
    Determine l8 band of individual product from product name
    from path to specific product file
    """

    prod_name = os.path.basename(prod_path)
    parts = prod_name.split('_')
    prod_name = f"{parts[-2]}_{parts[-1][:-4]}"

    prod_map = {
        "watermask": 'watermask',
        "waterprob": 'waterprob'
    }
    layer_name = prod_map[prod_name]
    return layer_name

def yaml_prep_water(scene_dir, original_yml):
    """
    Prepare individual wofs directory containing L8/S2/S1 cog water products.
    """
    # scene_name = scene_dir.split('/')[-2][:26]
    scene_name = scene_dir.split('/')[-2]
    print ( "Preparing scene {}".format(scene_name) )
    print ( "Scene path {}".format(scene_dir) )
    
    # find all cog prods
    prod_paths = glob.glob(scene_dir + '*water*.tif')
    # print ( 'paths: {}'.format(prod_paths) )
    # for i in prod_paths: print ( i )
    
    # date time assumed eqv for start and stop - this isn't true and could be 
    # pulled from .xml file (or scene dir) not done yet for sake of progression
    t0=parse(str(datetime.strptime(original_yml['extent']['center_dt'], '%Y-%m-%d %H:%M:%S')))
    # print ( t0 )
    t1=t0
    # print ( t1 )
    
    # name image product
    images = {
        prod_path.split('_')[-1][:9]: {
            'path': str(prod_path.split('/')[-1])
        } for prod_path in prod_paths
    }
    print ( images )

    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry(os.path.join(str(scene_dir), images['watermask']['path']))
#     extent = 
    print(projection, extent)
    
    new_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{scene_name}_water"))
    
    return {
        'id': new_id,
        'processing_level': original_yml['processing_level'],
        'product_type': "mlwater",
        'creation_dt': str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')),
        'platform': {  
            'code': original_yml['platform']['code']
        },
        'instrument': {
            'name': original_yml['instrument']['name']
        },
        'extent': {
            'coord': original_yml['extent']['coord'],
            'from_dt': str(t0),
            'to_dt': str(t1),
            'center_dt': str(t0 + (t1 - t0) / 2)
        },
        'format': {
            'name': 'GeoTiff'
        },
        'grid_spatial': {
            'projection': projection
        },
        'image': {
            'bands': images
        },
        'lineage': {
            'source_datasets': original_yml['lineage']['source_datasets'],
        }  
    }


def genprepmlwater(img_yml_path, lab_yml_path,
                   inter_dir='../tmp/data/intermediate/',
                   s3_bucket='public-eo-data',
                   s3_dir='common_sensing/fiji/mlwater_test/'):
    """
    optical_yaml_path: dc yml metadata of single image within S3 bucket
    summary_yaml_path: dc yml metadata of wofs-like summary product within S3 bucket
    """

    scene_name = os.path.dirname(img_yml_path).split('/')[-1]
        
    inter_dir = f"{inter_dir}{scene_name}_tmp/"
    os.makedirs(inter_dir, exist_ok=True)
    cog_dir = f"{inter_dir}{scene_name}/"
    os.makedirs(cog_dir, exist_ok=True)
                    
    des_band_refs = {
        "LANDSAT_8": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_7": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_5": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_4": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "SENTINEL_2": ['blue','green','red','nir','swir1','swir2','scene_classification'],
        "SENTINEL_1": ['vv','vh','layovershadow_mask'],
        "WOFS_SUMMARY": ['pc']}

    root = setup_logging()

    root.info(f"{scene_name} Starting")
    
    try: 

        try:
            root.info(f"{scene_name} Finding & Streaming Image & Labels Yamls")
            # get ymls directly
            img_yml = stream_yml(s3_bucket, img_yml_path)
            lab_yml = stream_yml(s3_bucket, lab_yml_path)
            img_sat = img_yml['platform']['code']
            lab_sat = lab_yml['platform']['code']
            # need to know agnostic ref and qa bands plus all bands from all srcs
            ref_channel = get_ref_channel(img_sat)
            qa_channel = get_qa_channel(img_sat)
            des_bands = des_band_refs[img_sat] + des_band_refs[lab_sat]
            root.info(f"{scene_name} Found & access yamls")
        except:
            root.exception(f"{scene_name} Yaml or band files can't be found")
            raise Exception('Streaming Error')

        try:
            root.info(f"{scene_name} Loading & Reformatting bands")
            # LOAD & PREP IMAGE & LABEL DATA
            #### SHOULD BE LAZY    ####
            # load all bands into same xr w/ same extent
            paths, bands = get_remote_band_paths(s3_bucket,[img_yml_path,lab_yml_path],des_bands)
            bands_data = load_bands(paths)
            xr_data = load_img(bands_data, bands)
            bands_data = None
            #### SHOULD BE LAZY ^^ ####            
            # catch s1 + scale + re-fromat dtype
            if img_sat == 'SENTINEL_1':
                att = xr_data.attrs
                xr_data = xr_data*100
                xr_data = xr_data.astype('uint16')
                xr_data.attrs = att
            else:
                att = xr_data.attrs
                xr_data = xr_data.astype('uint16')
                xr_data.attrs = att
        except:
            root.exception(f"{scene_name} Band data not loaded properly")
            raise Exception('Data formatting error')

        try:
            root.info(f"{scene_name} Applying masks")
            # VALID REGION MASKS
            validmask_img = get_valid(xr_data, img_sat) # img nd mask
            validmask_lab = get_valid(xr_data, lab_sat) # water nd mask
            validmask_train = validmask_img*validmask_lab # inner true mask
            
            # ASSIGN WATER/NON WATER CLASS LABELS
            water_thresh = 50 # 50% persistence in summary
            xr_data['pc'] = xr_data.pc.where((xr_data.pc < water_thresh) | (validmask_lab == False), 100) # fix > prob to water
            xr_data['waterclass'] = xr_data.pc.where((xr_data.pc >= water_thresh) | (validmask_lab == False), 0) # fix < prob to no water 
            xr_data = xr_data.drop(['pc'])
        
            # MASK TO TRAINING SAMPLES W/ IMPUTED ND
            train_data = xr_data # dup as use img 4 implementation later
            train_data = train_data.where(validmask_train == True, -9999).drop([qa_channel]) # apply inner mask

            unique, counts = np.unique(train_data.waterclass, return_counts=True)
            if (counts[0] < 500) | (counts[1] < 5000):
                raise Exception(f'no class labels should be >5000 for ok classifier. no. training class samples: {counts[0]}{counts[1]}')
        except:
            root.exception(f"{scene_name} Masks not applied")
            raise Exception('Data formatting error')
        
        try:
            root.info(f"{scene_name} Training")
            # SPEC & TRAIN MODEL
            Y = train_data.waterclass.stack(z=['x','y']) # stack into 1-d arr
            X = train_data.drop(['waterclass']).stack(z=['x','y']).to_array().transpose() # stack into transposed 2-d arr

            # very shallow classifier - this is a super easy problem & we want it to be fast
            wrapper = wrap(RandomForestClassifier(n_estimators=4, 
                                           bootstrap = True,
                                           max_features = 'sqrt',
                                           max_depth=5,
                                           n_jobs=2,
                                           verbose=2
                                          ))
            wrapper.estimator.fit(X, Y) # do training
        except:
            root.exception(f"{scene_name} Training failed")
            raise Exception('Model training error')
        
        try:
            root.info(f"{scene_name} Prediction")
            # MASK TO FULL VALID IMAGE FOR IMPLEMENTATION
            xr_data = xr_data.drop([qa_channel,'waterclass']) # not sure how these ended up in here(?)
            xr_data = xr_data.where(validmask_img == True, -9999) # apply just the img mask this time

            # PREDICT + ASSIGN CONFIDENCE
            X = xr_data.stack(z=['x','y']).to_array().transpose() # stack into transposed 2-d arr
            pred = wrapper.estimator.predict(X) # gen class predictions
            pred[pred==100] = 1
            prob = wrapper.estimator.predict_proba(X)[:,1]*100 # gen confidence in assigned labels as int

            # RESHAPE OUTPUTS INTO IMAGE
            vars_0 = [i for i in X.transpose().to_dataset(dim='variable').data_vars] # get list of vars within img
            X_t = X.transpose().to_dataset(dim='variable') # recreate xrds (but no unstacking yet as need to drop in model outputs)
            X_t[vars_0[0]].data = pred # add class predictions as first channel
            X_t[vars_0[1]].data = prob # add confidence as second channel
            X_t = X_t.rename({vars_0[0]:'water_mask',vars_0[1]:'water_prob'}).drop(vars_0[2:]).unstack('z').transpose().astype('int16') # rename + drop vars + unstack xy dims back to 3-d xrds + transpose predictions back into correct orientation
            X_t = X_t.where(validmask_img,-9999) # ensure probs rm 4 nd regions
            X_t.attrs = xr_data.attrs
            X_t.attrs['crs'] = xr_data.rio.crs
        except:
            root.exception(f"{scene_name} Prediction or re-shaping failed")
            raise Exception('Prediction error')
            
        try:
            root.info(f"{scene_name} Exporting water product")   
            # EXPORT
            inter_prodir = inter_dir + scene_name + '_mlwater/'
            os.makedirs(inter_prodir, exist_ok=True)
            out_mask_prod = inter_prodir + scene_name + '_watermask.tif'
            out_prob_prod = inter_prodir + scene_name + '_waterprob.tif'
            output_crs = xr_data.rio.crs

            export_xarray_to_geotiff(X_t, out_mask_prod, bands=['water_mask'], crs=output_crs, x_coord='x', y_coord='y', no_data=-9999)
            export_xarray_to_geotiff(X_t, out_prob_prod, bands=['water_prob'], crs=output_crs, x_coord='x', y_coord='y', no_data=-9999)
        except:
            root.exception(f"{scene_name} Water product export failed")
            raise Exception('Export error')
            
        try:
            root.info(f"{scene_name} Creating yaml")
            # CREATE YML
            create_yaml(inter_prodir, yaml_prep_water(inter_prodir, img_yml)) # assumes majority of meta copied from original product yml
        except:
            root.exception(f"{scene_name} yam not created")
            raise Exception('Yaml error')
            
        try:
            root.info(f"{scene_name} Uploading to S3 Bucket")
            # UPLOAD
            s3_upload_cogs(glob.glob(f'{inter_prodir}*'), s3_bucket, s3_dir)
        except:
            root.exception(f"{scene_name} Upload to S3 Failed")
            raise Exception('S3  upload error')
            
        xr_data = None
        att = None
        img_data = None
        validmask_img = None
        validmask_lab = None
        validmask_train = None
        class_data = None
        train_data = None
        wrapperper = None
        wrapper = None
        X = None
        Y = None
        pred = None
        prob = None
        vars_0 = None
        X_t = None

        clean_up(inter_dir)
        print('not boo')

    except Exception as e:
        logging.error(f"could not process {scene_name}, {e}", )

        xr_data = None
        att = None
        img_data = None
        validmask_img = None
        validmask_lab = None
        validmask_train = None
        class_data = None
        train_data = None
        wrapperper = None
        wrapper = None
        X = None
        Y = None
        pred = None
        prob = None
        vars_0 = None
        X_t = None

        clean_up(inter_dir)
        print('boo')

        
if __name__ == '__main__':

    genprepmlwater(
        "common_sensing/fiji/landsat_8/LC08_L1TP_074072_20190129/datacube-metadata.yaml",
        "common_sensing/fiji/wofs_summary/wofssummary_20130101_20140101/datacube-metadata.yaml"
    )