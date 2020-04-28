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

# ml stuff
from sklearn_xarray import wrap
from sklearn.ensemble import RandomForestClassifier

from . prep_utils import s3_list_objects, s3_download, s3_upload_cogs, create_yaml, cog_translate, get_geometry
from . dc_import_export import export_xarray_to_geotiff


def resamp_bands(xr, xrs):
    if xr.attrs['res'] == xrs[0].attrs['res']:
        return xr
    else:
        return xr.interp(x=xrs[0]['x'], y=xrs[0]['y'], method='nearest') # nearest as exclusively upsampling
    
def rename_bands(in_xr, des_bands, position):
    in_xr.name = des_bands[position]
    return in_xr

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
            (ds >= 0)
        )
    elif 'SENTINEL_1' in prod:
        good_quality = (
            (ds.vv != 0)
        )
    return good_quality

def get_ref_channel(prod):
    if ('LANDSAT' in prod) | ('SENTINEL_2' in prod): return 'swir1'
    elif 'SENTINEL_1' in prod: return 'vv'
    
def get_qa_channel(prod):
    if 'LANDSAT' in prod: return 'pixel_qa'
    elif 'SENTINEL_2' in prod: return 'scene_classification'
    elif 'SENTINEL_1' in prod: return 'layovershadow_mask'
    

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


def genprepmlwater(optical_yaml_path, summary_yaml_path, inter_dir='../data/', s3_bucket='public-eo-data', s3_dir='common_sensing/fiji/mlwater_test/',
            mask=None, output_crs=None):
    """
    optical_yaml_path: dc yml metadata of single image within S3 bucket
    summary_yaml_path: dc yml metadata of wofs-like summary product within S3 bucket
    """

        
    scene_name = os.path.dirname(optical_yaml_path).split('/')[-1]
        
    inter_dir = f"{inter_dir}{scene_name}_tmp/"
    os.makedirs(inter_dir, exist_ok=True)
    cog_dir = f"{inter_dir}{scene_name}/"
    os.makedirs(cog_dir, exist_ok=True)
    
    
    # Logging structure taken from - https://www.loggly.com/ultimate-guide/python-logging-basics/
    log_file = inter_dir+'log_file.txt'
    handler = logging.handlers.WatchedFileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(os.environ.get("LOGLEVEL", "DEBUG"))
    root.addHandler(handler)
    
    root.info(f"{scene_name} Starting")
        
        
    yml = f'{inter_dir}datacube-metadata.yaml'
    yml_summary = f'{inter_dir}datacube-metadata_watersummary.yaml'
        
    des_band_refs = {
        "LANDSAT_8": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_7": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_5": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_4": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "SENTINEL_2": ['blue','green','red','nir','swir1','swir2','scene_classification'],
        "SENTINEL_1": ['vv','vh','layovershadow_mask'],
        "WOFS_SUMMARY": ['pc']}

    try: 

        try:
            root.info(f"{scene_name} Finding & Downloading Image yml & data")
            if not os.path.exists(yml):
                s3_download(s3_bucket, optical_yaml_path, yml)
                with open (yml) as stream: yml_meta = yaml.safe_load(stream)
                satellite = yml_meta['platform']['code'] # helper to generalise masking 
                des_bands = des_band_refs[satellite]
                print(satellite, des_bands)
                band_paths_s3 = [os.path.dirname(optical_yaml_path)+'/'+yml_meta['image']['bands'][b]['path'] for b in des_bands ]
                band_paths_local = [inter_dir+os.path.basename(i) for i in band_paths_s3]
                for s3, loc in zip(band_paths_s3, band_paths_local): 
                    if not os.path.exists(loc):
                        s3_download(s3_bucket, s3, loc)
            elif os.path.exists(yml):
                with open (yml) as stream: yml_meta = yaml.safe_load(stream)
                satellite = yml_meta['platform']['code'] # helper to generalise masking 
                des_bands = des_band_refs[satellite]
                print(satellite, des_bands)

            # FIND & DOWNLOAD WATER SUMMARY DATA
            root.info(f"{scene_name} Finding & Downloading Water Summary yml & data")
            if not os.path.exists(yml_summary):
                s3_download(s3_bucket, summary_yaml_path, yml_summary)
                with open (yml_summary) as stream: yml_summary_meta = yaml.safe_load(stream)
                summary = yml_summary_meta['platform']['code']
                des_bands_summary = des_band_refs[summary]
                band_paths_s3 = [os.path.dirname(summary_yaml_path)+'/'+yml_summary_meta['image']['bands'][b]['path'] for b in des_bands_summary ]
                band_paths_local = [inter_dir+os.path.basename(i) for i in band_paths_s3]
                for s3, loc in zip(band_paths_s3, band_paths_local): 
                    if not os.path.exists(loc):
                        s3_download(s3_bucket, s3, loc)
            elif os.path.exists(yml_summary):
                s3_download(s3_bucket, summary_yaml_path, yml_summary)
                with open (yml_summary) as stream: yml_summary_meta = yaml.safe_load(stream)
                summary = yml_summary_meta['platform']['code']
                des_bands_summary = des_band_refs[summary]
                band_paths_s3 = [os.path.dirname(summary_yaml_path)+'/'+yml_summary_meta['image']['bands'][b]['path'] for b in des_bands_summary ]
                band_paths_local = [inter_dir+os.path.basename(i) for i in band_paths_s3]
                for s3, loc in zip(band_paths_s3, band_paths_local): 
                    if not os.path.exists(loc):
                        s3_download(s3_bucket, s3, loc)                        
            ref_channel = get_ref_channel(satellite)
            qa_channel = get_qa_channel(satellite)
        except:
            root.exception(f"{scene_name} Yaml or band files can't be found")
            raise Exception('Download Error')
            root.info(f"{scene_name} Found & Downloaded yml & data")

        try:
            root.info(f"{scene_name} Loading & Reformatting bands")
            # LOAD & PREP IMAGE DATA
            o_bands_data = [ xr.open_rasterio(inter_dir + yml_meta['image']['bands'][b]['path']) for b in des_bands ] # loading
            o_bands_data = [ resamp_bands(i, o_bands_data) for i in o_bands_data ] # resamp 2 match band 1
            bands_data = xr.merge([rename_bands(bd, des_bands, i) for i,bd in enumerate(o_bands_data)]).rename({'band': 'time'}) # ensure band names & dims consistent
            bands_data = bands_data.assign_attrs(o_bands_data[0].attrs) # crs etc. needed later
            bands_data['time'] = [datetime.strptime(yml_meta['extent']['center_dt'], '%Y-%m-%d %H:%M:%S')] # time dim needed for wofs
            img_data = bands_data.isel(time = 0).drop(['time'])
            bands_data = None
            o_bands_data = None
            # LOAD & PREP WATER SUMMARY DATA
            o_bands_data = [ xr.open_rasterio(inter_dir + yml_summary_meta['image']['bands'][b]['path']) for b in des_bands_summary ] # loading
            o_bands_data = [ resamp_bands(i, o_bands_data) for i in o_bands_data ]
            sum_data = xr.merge([rename_bands(bd, des_bands_summary, i) for i,bd in enumerate(o_bands_data)]).rename({'band': 'time'}) # ensure band names & dims consistent
            sum_data = sum_data.assign_attrs(o_bands_data[0].attrs) # crs etc. needed later
            sum_data['time'] = [datetime.strptime(yml_summary_meta['extent']['center_dt'], '%Y-%m-%d %H:%M:%S')] # time dim needed for wofs
            class_data = sum_data.isel(time = 0).drop(['time']).pc
            sum_data = None
            o_bands_data = None
        except:
            root.exception(f"{scene_name} Band data not loaded properly")
            raise Exception('Data formatting error')

        try:
            root.info(f"{scene_name} Reprojecting & alligning")
            # REPROJECT & ALIGN CRS+DIMS
            class_data = class_data.rio.reproject_match(img_data[ref_channel]) # repro + align grid
            img_data, class_data = xr.align(img_data, class_data, join="override") # force alignment of x,y precision

            if satellite == 'SENTINEL_1': # catch s1 and scall and re-fromat dtype
                att = img_data.attrs
                img_data = img_data*100
                img_data = img_data.astype('int16')
                img_data.attrs = att
        except:
            root.exception(f"{scene_name} Reprojecting & aligning failed")
            raise Exception('Reprojection error')

        try:
            root.info(f"{scene_name} Applying masks")
            # VALID REGION MASKS
            clearskymask_img = get_valid(img_data, satellite) # img nd mask
            clearskymask_class = get_valid(class_data, summary) # water nd mask
            clearskymask_train = clearskymask_class.where(clearskymask_class == False, False) # empty mask
            clearskymask_train = clearskymask_train.where((clearskymask_img == False) | (clearskymask_class == False), True) # inner true mask

            # ASSIGN WATER/NON WATER CLASS LABELS
            water_thresh = 50 # 50% persistence in annual summary
            class_data = class_data.where((class_data < water_thresh) | (clearskymask_class == False), 100) # fix > prob to water
            class_data = class_data.where((class_data >= water_thresh) | (clearskymask_class == False), 0) # fix < prob to no water 
        
            # MASK TO TRAINING SAMPLES W/ IMPUTED ND
            train_data = img_data # dup as use img 4 implementation later
            train_data['waterclass'] = class_data # add channel for water mask
            train_data = train_data.where(clearskymask_train == True, -9999).drop([qa_channel]) # apply inner mask

            unique, counts = np.unique(train_data.waterclass, return_counts=True)
            if (counts[0] < 500) | (counts[1] < 5000):
                root.exception(f'no class labels should be >5000 for ok classifier. no. training class samples: {counts[0]}{counts[1]}')
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
            wrapper = wrap(RandomForestClassifier(n_estimators=2, 
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
            img_data = img_data.drop([qa_channel,'waterclass']) # not sure how these ended up in here(?)
            img_data = img_data.where(clearskymask_img == True, -9999) # apply just the img mask this time

            # PREDICT + ASSIGN CONFIDENCE
            X = img_data.stack(z=['x','y']).to_array().transpose() # stack into transposed 2-d arr

            pred = wrapper.estimator.predict(X) # gen class predictions
            pred[pred==100] = 1 # refactor water from 100 to 1
            prob = wrapper.estimator.predict_proba(X)[:,2]*100 # gen confidence in assigned labels as int

            # RESHAPE OUTPUTS INTO IMAGE
            vars_0 = [i for i in X.transpose().to_dataset(dim='variable').data_vars] # get list of vars within img
            X_t = X.transpose().to_dataset(dim='variable') # recreate xrds (but no unstacking yet as need to drop in model outputs)
            X_t[vars_0[0]].data = pred # add class predictions as first channel
            X_t[vars_0[1]].data = prob # add confidence as second channel
            X_t = X_t.rename({vars_0[0]:'water_mask',vars_0[1]:'water_prob'}).drop(vars_0[2:]).unstack('z').transpose().astype('int16') # rename + drop vars + unstack xy dims back to 3-d xrds + transpose predictions back into correct orientation
            X_t = X_t.where(clearskymask_img,-9999) # ensure probs rm 4 nd regions
            X_t.attrs = img_data.attrs
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
            output_crs = f"EPSG:{X_t.attrs['crs'].split(':')[-1]}"
            export_xarray_to_geotiff(X_t, out_mask_prod, bands=['water_mask'], crs=output_crs, x_coord='x', y_coord='y', no_data=-9999)
            export_xarray_to_geotiff(X_t, out_prob_prod, bands=['water_prob'], crs=output_crs, x_coord='x', y_coord='y', no_data=-9999)
        except:
            root.exception(f"{scene_name} Water product export failed")
            raise Exception('Export error')
            
        try:
            root.info(f"{scene_name} Creating yaml")
            # CREATE YML
            create_yaml(inter_prodir, yaml_prep_water(inter_prodir, yml_meta)) # assumes majority of meta copied from original product yml
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

        root.removeHandler(handler)
        handler.close()
            
        class_data = None
        att = None
        img_data = None
        clearskymask_img = None
        clearskymask_class = None
        clearskymask_train = None
        class_data = None
        train_data = None
        wrapperper = None
        X = None     
        Y = None
        pred = None
        prob = None
        vars_0 = None
        X_t = None

#         # Tidy up log file to ensure upload
#         shutil.move(log_file, inter_prodir + 'log_file.txt')
#         s3_upload_cogs(glob.glob(inter_prodir + '*log_file.txt'), s3_bucket, s3_dir)
        
        shutil.rmtree(inter_dir)
        gc.collect()


        print('not boo')

    except Exception as e:
        print(e)
        traceback.print_exc()

        class_data = None
        att = None
        img_data = None
        clearskymask_img = None
        clearskymask_class = None
        clearskymask_train = None
        class_data = None
        train_data = None
        wrapperper = None
        X = None     
        Y = None
        pred = None
        prob = None
        vars_0 = None
        X_t = None

#         # Tidy up log file to ensure upload
#         shutil.move(log_file, inter_prodir + 'log_file.txt')
#         s3_upload_cogs(glob.glob(inter_prodir + '*log_file.txt'), s3_bucket, s3_dir)
        
        shutil.rmtree(inter_dir)
        gc.collect()
        print('boo')

        
    