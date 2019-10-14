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
import boto3
import csv
import pandas as pd
import logging
import math

try:
    from .prep_utils import *
except:
    from prep_utils import *
    
    
    
def get_s1_asf_urls(s1_name_list, tmp_csv):

    df = pd.DataFrame()
    
    num_parts = math.ceil(len(s1_name_list)/119)
    s1_name_lists = numpy.array_split(numpy.array(s1_name_list),num_parts)
    print([len(l) for l in s1_name_lists])
    
    for l in s1_name_lists:
        cmd = f"curl https://api.daac.asf.alaska.edu/services/search/param?granule_list={','.join(l)}\&output=csv > {tmp_csv}"
        p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()   
        
        try:
            df = df.append(pd.read_csv(tmp_csv), ignore_index=True)
        except Exception as e:
            print(e)
            
        os.remove(tmp_csv)
        
    return df.loc[df['Processing Level'] == 'GRD_HD']


def get_s1_asfurl(s1_name, download_dir):
    """
    Finds Alaska Satellite Facility download url for single S1_NAME Sentinel-1 scene. 

    :param s1_name: Scene ID for Sentinel Tile (i.e. "S1A_IW_SLC__1SDV_20190411T063207_20190411T063242_026738_0300B4_6882")
    :param download_dir: path to directory for downloaded S1 granule
    :return s1url:download url
    :return False: unable to find url
    """
    
    csv_out = download_dir + 'tmpquery.csv'
    
    cmd = "curl https://api.daac.asf.alaska.edu/services/search/param?granule_list={}\&output=csv > {}".format(s1_name, csv_out)
    p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    out = p.stdout.read()
    
    try:
        s1url = pd.read_csv(csv_out).URL.values[0]
    except:
        s1url = 'NaN'
        
    os.remove(csv_out)
    
    return s1url


def download_extract_s1_scene_asf(s1_name, download_dir):
    """
    Downloads single S1_NAME Sentinel-1 scene into DOWLOAD_DIR. 

    :param s1_name: Scene ID for Sentinel Tile (i.e. "S1A_IW_SLC__1SDV_20190411T063207_20190411T063242_026738_0300B4_6882")
    :param download_dir: path to directory for downloaded S1 granule
    :return:
    """
    
    if s1_name.endswith('.SAFE'):
        s1_name = s1_name[:-5]
    
    # Grab url for scene
    s1url = get_s1_asfurl(s1_name, download_dir)
    
    asf_user = os.getenv("ASF_USERNAME")
    asf_pwd = os.getenv("ASF_PWD")
    
    # Extract downloaded .zip file
    zipped = download_dir + s1_name + '.zip'

    if not os.path.exists(zipped) & (not os.path.exists(zipped.replace('.zip','.SAFE/'))):
        cmd = "wget -c --http-user={} --http-password='{}' '{}' -P {}".format(asf_user, asf_pwd, s1url, download_dir)        
        p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()    


#     if ( os.path.exists(zipped) ) & ( not os.path.exists(zipped.replace('.zip','.SAFE/')) ):
#     print(zipped, zipped.replace('.zip','.SAFE/'))
    if ( not os.path.exists(zipped.replace('.zip','.SAFE/')) ):
        print ( 'Extracting ESA scene: {}'.format(zipped) )
        zip_ref = zipfile.ZipFile(zipped, 'r')
        zip_ref.extractall(os.path.dirname(download_dir))
        zip_ref.close()  
    
#     # Remove zipped scene but onliy if unzipped 
#     if os.path.exists(zipped) & os.path.exists(zipped.replace('.SAFE/','.zip')):
#         print ( 'Deleting ESA scene zip: {}'.format(zipped.replace('.SAFE/','.zip')) )
#         os.remove(zipped.replace('.SAFE/','.zip'))

            
def conv_s1scene_cogs(noncog_scene_dir, cog_scene_dir, scene_name, overwrite=False):
    """
    Convert S2 scene products to cogs + validate.
    TBD whether consistent for L1C + L2A prcoessing levels.
    """
    
    if not os.path.exists(noncog_scene_dir):
        print('Cannot find non-cog scene directory: {}'.format(noncog_scene_dir))
    
    # create cog scene directory - replace with one lined os.makedirs(exists_ok=True)
    if not os.path.exists(cog_scene_dir):
        print ( 'Creating scene cog directory: {}'.format(cog_scene_dir) )
        os.mkdir(cog_scene_dir)
    
    
    cog_val = []
    
    des_prods = ["Gamma0_VV_db",
                 "Gamma0_VH_db",
                 "LayoverShadow_MASK_VH"] # to ammend once outputs finalised - TO DO*****
    
    # find all individual prods to convert to cog (ignore true colour images (TCI))
    prod_paths = glob.glob(noncog_scene_dir + '*TF_TC*/*.img') # - TO DO*****
    prod_paths = [x for x in prod_paths if os.path.basename(x)[:-4] in des_prods]

    for i in prod_paths: print (i)
    
    proc_list = []
    
    # iterate over prods to create parellel processing list
    for prod in prod_paths: 
             
        in_filename = prod
        out_filename = cog_scene_dir + scene_name + '_' + os.path.basename(prod)[:-4] + '.tif' # - TO DO*****
                
        # ensure input file exists
        if os.path.exists(in_filename):
            
            # ensure output cog doesn't already exist
            if not os.path.exists(out_filename):
                
                proc_list.append((in_filename, out_filename))
                conv_sgl_cog( in_filename, out_filename )
                
            else:
                print ( 'cog already exists: {}'.format(out_filename) )
        else:
            print ( 'cannot find product: {}'.format(in_filename) )
    n = 3
#     print(proc_list)
#     pool = multiprocessing.Pool(processes=5)
#     pool.starmap(conv_sgl_cog, proc_list)
    
    # return cog_val


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
    if ds is not None:
        b = ds.GetRasterBand(1)
        b.SetNoDataValue(0)
        b.FlushCache()
        b = None
        ds = None 
    else:
        print ('not updated nodata')

    # should inc. cog val...
    

def copy_s1_metadata(out_s1_prod, cog_scene_dir, scene_name):
    """
    Parse through S2 metadtaa .xml for either l1c or l2a S2 scenes.
    """
    
    if os.path.exists(out_s1_prod):
        
        meta_base = os.path.basename(out_s1_prod)
        n_meta = cog_scene_dir + scene_name + '_' + meta_base
        print ( "Copying original metadata file to cog dir: {}".format(n_meta) )
        if not os.path.exists(n_meta):
            shutil.copyfile(out_s1_prod, n_meta)
        else:
            print ( "Original metadata file already copied to cog_dir: {}".format(n_meta) )
    else:
        print ( "Cannot find orignial metadata file: {}".format(meta) )



def prepareS1(in_scene, s3_bucket='public-eo-data', s3_dir='yemen/Sentinel_1/', inter_dir='/tmp/data/intermediate/', source='asf'):
    """
    Prepare IN_SCENE of Sentinel-1 satellite data into OUT_DIR for ODC indexing. 

    :param in_scene: input Sentinel-1 scene name (either L1C or L2A) i.e. "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE"
    :param out_dir: output directory to drop COGs into.
    :param --inter: optional intermediary directory to be used for processing.
    :param --source: Api source to be used for downloading scenes. Defaults to gcloud. Options inc. 'gcloud', 'esahub', 'sedas' COMING SOON
    :return: None
    
    Assumptions:
    - etc.... tbd
    """
    
    # Need to handle inputs with and without .SAFE extension
    if not in_scene.endswith('.SAFE'):
        in_scene = in_scene + '.SAFE'
    # shorten scene name
    scene_name = in_scene[:32]

    # Unique inter_dir needed for clean-up
    inter_dir = inter_dir + scene_name +'_tmp/'
    print(inter_dir)
    # sub-dirs used only for accessing tmp files
    down_dir = inter_dir + in_scene + '/' 
    cog_dir = inter_dir + scene_name + '/'
    os.makedirs(cog_dir, exist_ok=True)
    # s1-specific relative inputs
    input_mani = inter_dir + in_scene + '/manifest.safe'
    inter_prod = inter_dir + scene_name + '_Orb_Cal_Deb_ML.dim'
    inter_prod_dir = inter_prod[:-4] + '.data/'
    out_prod1 = inter_dir + scene_name + '_Orb_Cal_Deb_ML_TF_TC_dB.dim'
    out_dir1 = out_prod1[:-4] + '.data/'
    out_prod2 = inter_dir + scene_name + '_Orb_Cal_Deb_ML_TF_TC_lsm.dim'
    out_dir2 = out_prod2[:-4] + '.data/'
    
    # Logging structure taken from - https://www.loggly.com/ultimate-guide/python-logging-basics/
    log_file = inter_dir+'log_file.txt'
    handler = logging.handlers.WatchedFileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(os.environ.get("LOGLEVEL", "DEBUG"))
    root.addHandler(handler)

    root.info('{} {} Starting'.format(in_scene, scene_name))
    
    try:
        
        snap_gpt = os.environ['SNAP_GPT']
        int_graph_1 = os.environ['S1_PROCESS_P1'] # ENV VAR        
        int_graph_2 = os.environ['S1_PROCESS_P2'] # ENV VAR 
        
        # DOWNLOAD
        if source == "asf":
            download_extract_s1_scene_asf(in_scene, inter_dir)
            root.info('{} {} DOWNLOADED from ASF'.format(in_scene, scene_name))
            # TO DO - add something on current bandwidth...
        elif source == "esa":
            root.exception('{} {} CANNOT DOWNLOAD from ESA (yet..)'.format(in_scene, scene_name))
        
        # SNAP - logic to check if exists, 
        if not os.path.exists(out_prod2):
            cmd = f"{snap_gpt} {int_graph_1} -Pinput_grd={input_mani} -Poutput_ml={inter_prod}"
            print(cmd)
            p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
            out = p.stdout.read()
            root.info('{} {} PROCESSED'.format(in_scene, scene_name))
            print('pt2')
            cmd = f"{snap_gpt} {int_graph_2} -Pinput_ml={inter_prod} -Poutput_db={out_prod1} -Poutput_ls={out_prod2}"
            print(cmd)
            p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
            out = p.stdout.read()
            root.info('{} {} PROCESSED'.format(in_scene, scene_name))
            
        # CONVERT TO COGS TO TEMP COG DIRECTORY**
        conv_s1scene_cogs(inter_dir, cog_dir, scene_name)
        root.info('{} {} COGGED MASK'.format(in_scene, scene_name))
        
        # PARSE METADATA TO TEMP COG DIRECTORY**
        copy_s1_metadata(out_prod1, cog_dir, scene_name)
        copy_s1_metadata(out_prod2, cog_dir, scene_name)
        root.info('{} {} MTD'.format(in_scene, scene_name))
        
        # GENERATE YAML WITHIN TEMP COG DIRECTORY**
        create_yaml(cog_dir, 's1')
        root.info('{} {} YAML'.format(in_scene, scene_name))
        
        # MOVE COG DIRECTORY TO OUTPUT DIRECTORY
        s3_upload_cogs(glob.glob(cog_dir + '*'), s3_bucket, s3_dir)
        root.info('{} {} UPLOADED'.format(in_scene, scene_name))
        
        root.removeHandler(handler)
        handler.close()
        
        shutil.move(log_file, cog_dir + 'log_file.txt')
        s3_upload_cogs(glob.glob(cog_dir + '*log_file.txt'), s3_bucket, s3_dir)
                
        # DELETE ANYTHING WITIN TEH TEMP DIRECTORY
        cmd = 'rm -frv {}'.format(inter_dir)
        p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()

    except Exception as e:
        print('boo', e)
        logging.exception("Exception occurred")
        root.removeHandler(handler)
        handler.close()
        
        shutil.move(log_file, cog_dir + 'log_file.txt')  
        
        s3_upload_cogs(glob.glob(cog_dir + '*log_file.txt'), s3_bucket, s3_dir)        
                
        cmd = 'rm -frv {}'.format(inter_dir)
        p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()

# if __name__ == '__main__':

#     prepareS2()

