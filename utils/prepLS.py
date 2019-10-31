import os
import time
from tqdm import tqdm
try:
    from google.cloud import storage
except:
    print('no gsutils installed')
import glob
try:
    from sentinelsat import SentinelAPI
except:
    print('sentinelsat not installed')
import zipfile
import shutil
import rasterio
import numpy
from subprocess import Popen, PIPE, STDOUT
import click
import multiprocessing
from multiprocessing import pool
from multiprocessing.pool import ThreadPool as Pool
from itertools import product
import csv
import logging
import logging.handlers
from sys import exit
import requests
import tarfile



try:
    from .prep_utils import *
except:
    from prep_utils import *

    
    
def download_extract_ls_url(ls_url, down_tar, untar_dir):
    """
    Download a landsat from ESPA url. 
    Assumes esa hub creds stored as env variables.
    
    :param scene_uuid: S2 download uuid from sentinelsat query
    :param down_dir: directory in which to create a downloaded product dir
    :param original_scene_dir: 
    :return: 
    """
    if not os.listdir(untar_dir):
        
        if not os.path.exists(down_tar):
            print (f"Downloading tar.gz: {down_tar}")
            resp = requests.get(ls_url)
            open(down_tar, 'wb').write(resp.content)
            
        print(f"Extracting tar.gz: {down_tar}")
        tar = tarfile.open(down_tar, "r:gz")
        tar.extractall(path=untar_dir)
        tar.close()
    
    else:
        print ( f"Scene already downloaded and extracted: {untar_dir}")

def conv_lsscene_cogs(untar_dir, cog_dir, overwrite=False):
    """
    Convert products to cogs [+ validate TBC].
    
    :param original_scene_dir: Downloaded S2 product directory (i.e. via ESA or GCloud; assumes .SAFE structure) 
    :param cog_scene_dir: directory in which to create the output COGs
    :param scene_name: shortened S2 scene name (i.e. S2A_MSIL2A_20190124T221941_T60KYF from S2A_MSIL2A_20190124T221941_N0211_R029_T60KYF_20190124T234344)
    :param overwrite: Binary for whether to overwrite or skip existing COG files)
    :return: 
    """
    
    if not os.path.exists(untar_dir):
        print('Cannot find original scene directory: {}'.format(untar_dir))
    
    # create cog scene directory
    if not os.path.exists(cog_dir):
        print ( 'Creating scene cog directory: {}'.format(cog_dir) )
        os.mkdir(cog_dir)
    
    cog_val = []

    prod_paths = glob.glob(f"{untar_dir}/*.tif")

    for i in prod_paths: print (i)
    
#     proc_list = []

    # iterate over prods to create parellel processing list
    for prod in prod_paths: 
             
        in_filename = prod
        out_filename = f"{cog_dir}{os.path.basename(in_filename)[:-4]}.tif"
#         out_filename = cog_scene_dir + scene_name + prod[-12:-4] + '.tif'
                
        # ensure input file exists
        if os.path.exists(in_filename):
            
            # ensure output cog doesn't already exist
            if not os.path.exists(out_filename):
                
#                 proc_list.append((in_filename, out_filename))
                conv_sgl_cog( in_filename, out_filename )
                
            else:
                print ( 'cog already exists: {}'.format(out_filename) )
        else:
            print ( 'cannot find product: {}'.format(in_filename) )
#     n = 3
#     print(proc_list)
#     pool = multiprocessing.Pool(processes=5)
#     pool.starmap(conv_sgl_cog, proc_list)
    
    # return cog_val


def conv_sgl_cog(in_path, out_path):
    """
    Convert a single input file to COG format. Default settings via cogeo repository (funcs within prep_utils). 
    COG val TBC
    
    :param in_path: path to non-cog file
    :param out_path: path to new cog file
    :return: 
    """
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
    if ds is not None:
        b = ds.GetRasterBand(1)
        b.SetNoDataValue(0)
        b.FlushCache()
        b = None
        ds = None 
    else:
        print ('not updated nodata')

    # should inc. cog val...

        
def copy_l8_metadata(untar_dir, cog_dir):
    """
    Parse through LS metadata files.
    
    :param original_scene_dir: downloaded S2 dir in which to find original metadata (MTD_*.xml) file.
    :param cog_scene_dir: dir in which to copy MTD into
    :param scene_name: shortened S2 scene name (i.e. S2A_MSIL2A_20190124T221941_T60KYF from S2A_MSIL2A_20190124T221941_N0211_R029_T60KYF_20190124T234344)
    :return: 
    """
    metas = [ fn for fn in glob.glob(f"{untar_dir}*") if (".tif" not in os.path.basename(fn)) & ("." in os.path.basename(fn))]
    print(metas)
    
    if metas:
        for meta in metas:
            
            n_meta = f"{cog_dir}{os.path.basename(meta)}"
            print ( 'native_meta: {}'.format(n_meta) )

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
    else:
        print (" No metadata to copy")

        
def prepareLS(ls_url, s3_bucket='public-eo-data', s3_dir='common_sensing/fiji/default', inter_dir='/tmp/data/intermediate/', prodlevel='L2A'):
    """
    Prepare IN_SCENE of Sentinel-2 satellite data into OUT_DIR for ODC indexing. 

    :param in_scene: input Sentinel-2 scene name (either L1C or L2A) i.e. "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410[.SAFE]"
    :param s3_bucket: name of the s3 bucket in which to upload preppared products
    :param s3_dir: bucket dir in which to upload prepared products
    :param inter_dir: dir in which to store intermeriary products - this will be nuked at the end of processing, error or not
    :param --prodlevel: Desired Sentinel-2 product level. Defaults to 'L1C'. Use 'L2A' for ARD equivalent
    :return: None
    
    Assumptions:
    - env set at SEN2COR_8: i.e. Sen2Cor-02.08.00-Linux64/bin/L2A_Process"
    - env set COPERNICUS_USERNAME
    - env set COPERNICUS_PWD
    - env set AWS_ACCESS
    - env set AWS_SECRET
    """
    
    down_basename = ls_url.split('/')[-1]
    scene_name = f"{down_basename[:4]}_L1TP_{down_basename[4:10]}_{down_basename[10:18]}"
    inter_dir = f"{inter_dir}{scene_name}_tmp/"
    os.makedirs(inter_dir, exist_ok=True)
    down_tar = f"{inter_dir}{down_basename}"
    untar_dir = f"{inter_dir}{scene_name}_untar/"
    os.makedirs(untar_dir, exist_ok=True)
    cog_dir = f"{inter_dir}{scene_name}/"
    os.makedirs(cog_dir, exist_ok=True)
    print(f"scene: {scene_name}\nuntar: {untar_dir}\ncog_dir{cog_dir}")
    
    # Logging structure taken from - https://www.loggly.com/ultimate-guide/python-logging-basics/
    log_file = inter_dir+'log_file.txt'
    handler = logging.handlers.WatchedFileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(os.environ.get("LOGLEVEL", "DEBUG"))
    root.addHandler(handler)
    
    root.info(f"{scene_name} Starting")
    
    try:
        
        try:
            root.info(f"{scene_name} DOWNLOADING via ESPA")
            download_extract_ls_url(ls_url, down_tar, untar_dir)
            root.info(f"{scene_name} DOWNLOADed + EXTRACTED")
        except:
            root.exception(f"{scene_name} CANNOT BE FOUND")
            raise Exception('Dwonload Error')
        
        try:
            root.info(f"{scene_name} Converting COGs")
            conv_lsscene_cogs(untar_dir, cog_dir)
            root.info(f"{scene_name} COGGED")
        except:
            root.exception(f"{scene_name} CANNOT BE COGGED")
            raise Exception('COG Error')
            
        try:
            root.info(f"{scene_name} Copying medata")
            copy_l8_metadata(untar_dir, cog_dir)
            root.info(f"{scene_name} Copied medata")
        except:
            root.exception(f"{scene_name} metadata not copied")
            raise Exception('Metadata copy error')

        try:
            root.info(f"{scene_name} Creating yaml")
            create_yaml(cog_dir, 'l8')
            root.info(f"{scene_name} Created yaml")
        except:
            root.exception(f"{scene_name} yam not created")
            raise Exception('Yaml error')


        # MOVE COG DIRECTORY TO OUTPUT DIRECTORY
        try:
            root.info(f"{scene_name} Uploading to S3 Bucket")
            s3_upload_cogs(glob.glob(cog_dir + '*'), s3_bucket, s3_dir)
            root.info(f"{scene_name} Uploaded to S3 Bucket")
        except:
            root.exception(f"{scene_name} Upload to S3 Failed")
            raise Exception('S3  upload error')

        root.removeHandler(handler)
        handler.close()
        
        # Tidy up log file to ensure upload
        shutil.move(log_file, cog_dir + 'log_file.txt')
        s3_upload_cogs(glob.glob(cog_dir + '*log_file.txt'), s3_bucket, s3_dir)
                
        # DELETE ANYTHING WITIN TEH TEMP DIRECTORY
        cmd = 'rm -frv {}'.format(inter_dir)
        p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()
                
        print('not boo')
            
    except:
        print('boo')
        root.exception("Processing INCOMPLETE so tidying up")
        root.removeHandler(handler)
        handler.close()

        shutil.move(log_file, cog_dir + 'log_file.txt')
        
        s3_upload_cogs(glob.glob(cog_dir + '*log_file.txt'), s3_bucket, s3_dir)        
                
        cmd = 'rm -frv {}'.format(inter_dir)
        p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()
