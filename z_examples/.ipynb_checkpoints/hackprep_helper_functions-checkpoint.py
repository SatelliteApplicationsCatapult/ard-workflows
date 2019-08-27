import sys

import numpy

import rasterio
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from rasterio.shutil import copy

from osgeo import gdal

import os
import uuid
from pathlib import Path
from xml.etree import ElementTree  # should use cElementTree..

import yaml
from dateutil import parser
from dateutil.parser import parse
from osgeo import osr
import glob
from datetime import datetime

#from multiprocessing.pool import ThreadPool as Pool
#from functools import partial
import multiprocessing
from itertools import product


import zipfile
import shutil

from sentinelsat import SentinelAPI

from .cogeo import *

import boto3
import os


def get_geometry(path):
    """
    function stolen and unammended
    """
    with rasterio.open(path) as img:
        left, bottom, right, top = img.bounds
        crs = str(str(getattr(img, 'crs_wkt', None) or img.crs.wkt))
        corners = {
            'ul': {
                'x': left,
                'y': top
            },
            'ur': {
                'x': right,
                'y': top
            },
            'll': {
                'x': left,
                'y': bottom
            },
            'lr': {
                'x': right,
                'y': bottom
            }
        }
        projection = {'spatial_reference': crs, 'geo_ref_points': corners}

        spatial_ref = osr.SpatialReference(crs)
        t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

        def transform(p):
            lon, lat, z = t.TransformPoint(p['x'], p['y'])
            return {'lon': lon, 'lat': lat}

        extent = {key: transform(p) for key, p in corners.items()}

        return projection, extent

    

def band_name_s1(prod_path):
    """
    Determine polarisation of individual product from product name 
    from path to specific product file
    """
    # print ( "Product path is: {}".format(prod_path) )
    
    prod_name = str(prod_path.split('/')[-1])
    # print ( "Product name is: {}".format(prod_name) )

    if 'VH' in str(prod_name):
        layername = 'vh'
    if 'VV' in str(prod_name):
        layername = 'vv'
        
    return layername



def band_name_s2(prod_path):
    """
    Determine s2 band of individual product from product name from 
    path to specific product file
    """
    # print ( "Product path is: {}".format(prod_path) )
    
    prod_name = str(os.path.basename(prod_path))
    # print ( "Product name is: {}".format(prod_name) )
    prod_name = prod_name[-11:-4]
    
    prod_map = {
        "AOT_10m": 'aerosol_optical_thickness',
        "B01_60m": 'coastal_aerosol',
        "B02_10m": 'blue',
        "B03_10m": 'green',
        "B04_10m": 'red',
        "B05_20m": 'vegetation_red_edge_1',
        "B06_20m": 'vegetation_red_edge_2',
        "B07_20m": 'vegetation_red_edge_3',
        "B08_10m": 'nir',
        "B8A_20m": 'water_vapour',
        "B09_60m": 'swir_1',
        "B11_20m": 'swir_2',
        "B12_20m": 'narrow_nir',
        "SCL_20m": 'scene_classification',
        "WVP_10m": 'wvp'       
    }   
        
    layername = prod_map[prod_name]
    
    # print ( layername )

    return layername



def yaml_prep_s1(scene_dir):
    """
    Prepare individual S1 scene directory containing S1 products
    note: doesn't inc. additional ancillary products such as incidence 
    angle or layover/foreshortening masks
    """
    scene_name = scene_dir.split('/')[-2]
    print ( "Preparing scene {}".format(scene_name) )
    print ( "Scene path {}".format(scene_dir) )
    
    prod_paths = glob.glob(scene_dir + '*dB.tif')
    # print ( "Preparing scene {}".format(prod_paths) ) 
    
    t0=parse(str( datetime.strptime(prod_paths[0].split("_")[-10].split(".")[0], '%Y%m%dT%H%M%S')))
    # print ( t0 )
    t1=t0
    # print ( t1 )

    # get polorisation from each image product (S1 band)
    # should be replaced with a more concise, generalisable parsing
    images = {
        band_name_s1(prod_path): {
            'path': str(prod_path.split('/')[-1])
        } for prod_path in prod_paths
    }
    # print ( images )
    
    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry('/'.join([str(scene_dir), images['vv']['path']]))
    
    # format metadata (i.e. construct hashtable tree for syntax of file interface)
    return {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name)),
        'processing_level': "sac_snap_ard",
        'product_type': "gamma0",
        'creation_dt': str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')),
        'platform': {
            'code': 'SENTINEL_1'
        },
        'instrument': {
            'name': 'SAR'
        },
        'extent': {
            'coord': extent,
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
            'source_datasets': {},
        }  

    }



def yaml_prep_s2(scene_dir):
    """
    Prepare individual S2 scene directory containing S2 cog products converted
    from ESA-disseminated L2A scenes.
    note: aims to align with usgs landsat indexing, core difference lies in 
    ommission of qa_band, however the scene classification file (SCL) addresses 
    a deal of these reqs.
    """
    # scene_name = scene_dir.split('/')[-2][:26]
    scene_name = scene_dir.split('/')[-2]
    print ( "Preparing scene {}".format(scene_name) )
    print ( "Scene path {}".format(scene_dir) )
    
    # find all cog prods
    prod_paths = glob.glob(scene_dir + '*.tif')
    # print ( 'paths: {}'.format(prod_paths) )
    # for i in prod_paths: print ( i )
    
    # date time assumed eqv for start and stop - this isn't true and could be 
    # pulled from .xml file (or scene dir) not done yet for sake of progression
    t0=parse(str( datetime.strptime(prod_paths[0].split("_")[-4], '%Y%m%dT%H%M%S')))
    # print ( t0 )
    t1=t0
    # print ( t1 )
    
    # get polorisation from each image product (S2 band)
    images = {
        band_name_s2(prod_path): {
            'path': str(prod_path.split('/')[-1])
        } for prod_path in prod_paths
    }
    # print ( images )
    
    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry('/'.join([str(scene_dir), images['blue']['path']]))
    
    # parse esa l2a prod metadata file for reference
    scene_genesis =  glob.glob(scene_dir + '*MTD_MSIL2A.xml')[0]
    if os.path.exists(scene_genesis):
        scene_genesis = os.path.basename(scene_genesis)
    else:
        scene_genesis = ' '
    
    new_id = str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name))
    print ('New uuid: {}'.format(new_id))
    
    return {
        'id': new_id,
        'processing_level': "esa_l2a2cog_ard",
        'product_type': "optical_ard",
        'creation_dt': str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')),
        'platform': {  
            'code': 'SENTINEL_2'
        },
        'instrument': {
            'name': 'MSI'
        },
        'extent': {
            'coord': extent,
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
            'source_datasets': scene_genesis,
        }  

    }
        
    
    
def create_yaml(scene_dir, sensor):
    """
    Create yaml for single scene directory containing cogs.
    """
        
    if sensor == 's1':
        metadata = prep_dataset_yaml_s1(scene_dir)

    elif sensor == 's2':
        metadata = yaml_prep_s2(scene_dir)
                        
    yaml_path = str(scene_dir + 'datacube-metadata.yaml')
    
    # not sure why default_flow_style is now required - strange...
    with open(yaml_path, 'w') as stream:
        yaml.dump(metadata, stream, default_flow_style=False)
        
    print ( 'Created yaml: {}'.format(yaml_path) )
        
        

def download_extract_s2(scene_uuid, down_dir, original_scene_dir):
    """
    Download a single S2 scene from ESA via sentinelsat 
    based upon uuid. 
    """

    # if unzipped .SAFE file doesn't exist then we must do something
    if not os.path.exists(original_scene_dir):
        
        # if downloaded .zip file doesn't exist then download it
        if not os.path.exists(original_scene_dir.replace('.SAFE/','.zip')):
            print ( 'Downloading ESA scene zip: {}'.format(os.path.basename(original_scene_dir)) )
            esa_api = SentinelAPI('tmj21','Welcome12!')            
            esa_api.download(scene_uuid, down_dir, checksum=True)
    
        # extract downloaded .zip file
        print ( 'Extracting ESA scene: {}'.format(original_scene_dir) )
        zip_ref = zipfile.ZipFile(original_scene_dir.replace('.SAFE/','.zip'), 'r')
        zip_ref.extractall(os.path.dirname(down_dir))
        zip_ref.close()        
    
    else:
        print ( 'ESA scene already extracted: {}'.format(original_scene_dir) )
    
    # remove zipped scene but onliy if unzipped 
    if os.path.exists(original_scene_dir) & os.path.exists(original_scene_dir.replace('.SAFE/','.zip')):
        print ( 'Deleting ESA scene zip: {}'.format(original_scene_dir.replace('.SAFE/','.zip')) )
        os.remove(original_scene_dir.replace('.SAFE/','.zip'))

    
def copy_s2_metadata(original_scene_dir, cog_scene_dir, scene_name):
    """
    Parse through S2 metadtaa .xml for either l1c or l2a S2 scenes.
    """
    
    meta_base = 'MTD_MSIL2A.xml'
    meta = original_scene_dir + meta_base
    # print ( 'native_meta: {}'.format(meta) )
    n_meta = cog_scene_dir + scene_name + '_' + meta_base
    # print ( 'native_meta: {}'.format(n_meta) )
    
    # check meta file exists
    if os.path.exists(meta):
        # check cp doesn't exist
        if not os.path.exists(n_meta):
            print ( "Copying original metadata file to cog dir: {}".format(n_meta) )
            shutil.copyfile(meta, n_meta)
        else:
            print ( "Original metadata file already copied to cog_dir: {}".format(n_meta) )
    else:
        print ( "Cannot find orignial metadata file: {}".format(meta) )

    
def conv_s2scene_cogs(original_scene_dir, cog_scene_dir, scene_name, overwrite=False):
    """
    Convert S2 scene products to cogs + validate.
    TBD whether consistent for L1C + L2A prcoessing levels.
    """
    
    # create cog scene directory
    if not os.path.exists(cog_scene_dir):
        print ( 'Creating scene cog directory: {}'.format(cog_scene_dir) )
        os.mkdir(cog_scene_dir)
    else:
        print ( 'Scene cog directory already exists so passing: {}'.format(scene_dir) )
    
    
    cog_val = []
    
    des_prods = ["AOT_10m", "B01_60m", "B02_10m", "B03_10m", "B04_10m", "B05_20m", "B06_20m",
                 "B07_20m", "B08_10m", "B8A_20m", "B09_60m", "B11_20m", "B12_20m", "SCL_20m",
                 "WVP_10m"]
    
    # find all individual prods to convert to cog (ignore true colour images (TCI))
    prod_paths = glob.glob(original_scene_dir + 'GRANULE/*/IMG_DATA/*/*.jp2')
    prod_paths = [x for x in prod_paths if x[-11:-4] in des_prods]
    
    
    # for i in prod_paths: ( print i )
    
    proc_list = []
    
    # iterate over prods to create parellel processing list
    for prod in prod_paths: 
     
        in_filename = prod
        out_filename = cog_scene_dir + scene_name + prod[-12:-4] + '.tif'
                
        # ensure input file exists
        if os.path.exists(in_filename):
            
            # ensure output cog doesn't already exist
            if not os.path.exists(out_filename):
                
                conv_sgl_cog( in_filename, out_filename )
                
            else:
                print ( 'cog already exists: {}'.format(out_filename) )
        else:
            print ( 'cannot find product: {}'.format(in_filename) )
            
    else:
        print ( 'all prods already cogs' )
            
    # return cog_val


#def conv_coglist(in_out_pairs):
def conv_sgl_cog(in_path, out_path):
    
    print (in_path, out_path)    
    # set default cog profile (as recommended by alex leith)
    cog_profile = {
        'driver': 'GTiff',
        'interleave': 'pixel',
        'tiled': True,
        'blockxsize': 512,
        'blockysize': 512,
        'compress': 'DEFLATE',
        'predictor': 2,
        'zlevel': 9
    }    
        
    cog_translate(
        in_path,
        out_path,
        cog_profile,
        overview_level=5,
        overview_resampling='average'
    )
    
    ds = gdal.Open(in_path, gdal.GA_Update)
    if ds is None:
        print ('not updated nodata')

    b = ds.GetRasterBand(1)
    b.SetNoDataValue(0)
    b.FlushCache()
    b = None
    ds = None
    
    
def s3_single_upload(in_path, s3_path, s3_bucket):
    """
    put a file into S3 from the local file system.

    :param in_path: a path to a file on the local file system
    :param s3_path: where in S3 to put the file.
    :return: None
    """
    
    # prep session & creds
    access = 'AKIAUSAVCCLQ5NF7AP7P'
    secret = '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'
    
    session = boto3.Session(
        access,
        secret,
    )
    s3 = session.resource('s3',region_name='eu-west-2')
    client = session.client('s3')
    bucket = s3.Bucket(s3_bucket)
    gb = 1024 ** 3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access,
        aws_secret_access_key=secret
    )
    
    # Ensure that multipart uploads only happen if the size of a transfer is larger than
    # S3's size limit for non multipart uploads, which is 5 GB. we copy using multipart 
    # at anything over 4gb
    transfer_config = boto3.s3.transfer.TransferConfig(multipart_threshold=2 * gb,
                                                       max_concurrency=10, 
                                                       multipart_chunksize=2 * gb,
                                                       use_threads=True) 
    
    print ( 'Local source file: {}'.format(in_path) )
    print ( 'S3 target file: {}'.format(s3_path) )
    
    if not os.path.exists(s3_path): # doesn't work on s3... better function to do this...
        print ( 'Start: {} {} '.format(in_path, str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))) )
                
        transfer = boto3.s3.transfer.S3Transfer(client=s3_client, config=transfer_config)
        transfer.upload_file(in_path, bucket.name, s3_path)  
        
        print ( 'Finish: {} {} '.format(in_path, str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))) )
        
    

def s3_upload_cogs(in_paths, s3_bucket, s3_dir):

    # create upload lists for multi-threading
    out_paths = [ s3_dir + i.split('/')[-2] + '/' + i.split('/')[-1] 
                 for i in in_paths ]
        
    upload_list = [(in_path, out_path, s3_bucket) 
                   for in_path, out_path in zip(in_paths, out_paths)]
    
    for i in upload_list: print ( i )
    
    # parallelise upload
    pool = multiprocessing.Pool(processes=5)
    pool.starmap(s3_single_upload, upload_list)
    
    
def tidy_up(original_scene_dir):
    """
    Delete intermediary files for a single scene.
    """¶

    
def sen2cor_correction(sen2cor, scene_dir, out_dir):
    """
    Run sen2cor on input S2 L1C product directory (must be unzipped).
    """
    cmd = sen2cor + ' ' + '--output_dir ' + out_dir + ' ' + scene_dir
    os.system(cmd)
    #scene.replace('_MSIL1C', '_MSIL2A')
    # scene_2a_mv = scene_2a.replace('L1C_gcloud', 'L2A_sen2cor')
    # os.system("mv %s %s" % (scene_2a, scene_2a_mv))
    
