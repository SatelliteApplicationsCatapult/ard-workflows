import os
import time
from tqdm import tqdm
from google.cloud import storage
import glob
from sentinelsat import SentinelAPI
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


try:
    from .cogeo import *
except:
    from cogeo import *
try:
    from .yamlUtils import *
except:
    from yamlUtils import *
    

def get_s1_asfurl(s1_name, download_dir):
    """
    Finds Alaska Satellite Facility download url for single S1_NAME Sentinel-1 scene. 

    :param s1_name: Scene ID for Sentinel Tile (i.e. "S1A_IW_SLC__1SDV_20190411T063207_20190411T063242_026738_0300B4_6882")
    :param download_dir: path to directory for downloaded S1 granule
    :return s1url:download url
    :return False: unable to find url
    """
    
    if s1_name.endswith('.SAFE'):
        s1_name = s1_name[:-5]
    
    csv_out = download_dir + 'tmpquery.csv'
    
    cmd = "curl https://api.daac.asf.alaska.edu/services/search/param?granule_list={}\&output=csv > {}".format(s1_name, csv_out)
    p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    out = p.stdout.read()
    
    s1url = pd.read_csv(csv_out).URL.values[0]
    
    os.remove(csv_out)
    
    return s1url


def download_s1_scene(s1_name='S1A_IW_SLC__1SDV_20190411T063207_20190411T063242_026738_0300B4_6882', download_dir='../S1_ARD/'):
    """
    Downloads single S1_NAME Sentinel-1 scene into DOWLOAD_DIR. 

    :param s1_name: Scene ID for Sentinel Tile (i.e. "S1A_IW_SLC__1SDV_20190411T063207_20190411T063242_026738_0300B4_6882")
    :param download_dir: path to directory for downloaded S1 granule
    :return:
    """
    
    # Grab url for scene
    s1_url = get_s1_asfurl(s1_name, download_dir)
    
    asf_user = os.getenv("ASF_USERNAME")
    asf_pwd = os.getenv("ASF_PWD")
    
    # Extract downloaded .zip file
    zipped = download_dir + s1_name + '.zip'

#     if not os.path.exists(zipped):
#         # Download
#         cmd = "wget -c --http-user={} --http-password='{}' '{}' -P {}".format(asf_user, asf_pwd, s1url, download_dir)        
#         p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
#         out = p.stdout.read()    
    
    print ( 'Extracting ESA scene: {}'.format(zipped) )
    zip_ref = zipfile.ZipFile(zipped.replace('.SAFE/','.zip'), 'r')
    zip_ref.extractall(os.path.dirname(download_dir))
    zip_ref.close()  
    
#     # Remove zipped scene but onliy if unzipped 
#     if os.path.exists(zipped) & os.path.exists(zipped.replace('.SAFE/','.zip')):
#         print ( 'Deleting ESA scene zip: {}'.format(zipped.replace('.SAFE/','.zip')) )
#         os.remove(zipped.replace('.SAFE/','.zip'))

    
def download_extract_s2_esa(scene_uuid, down_dir, original_scene_dir):
    """
    Download a single S2 scene from ESA via sentinelsat 
    based upon uuid. 
    """
    
    # if unzipped .SAFE file doesn't exist then we must do something
    if not os.path.exists(original_scene_dir):
        
        # if downloaded .zip file doesn't exist then download it
        if not os.path.exists(original_scene_dir.replace('.SAFE/','.zip')):
            print ( 'Downloading ESA scene zip: {}'.format(os.path.basename(original_scene_dir)) )

            copernicus_pwd=os.getenv("COPERNICUS_USERNAME")
            copernicus_username=os.getenv("COPERNICUS_PWD")
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
    
            
def conv_s2scene_cogs(original_scene_dir, cog_scene_dir, scene_name, overwrite=False):
    """
    Convert S2 scene products to cogs + validate.
    TBD whether consistent for L1C + L2A prcoessing levels.
    """
    
    if not os.path.exists(original_scene_dir):
        print('Cannot find original scene directory: {}'.format(original_scene_dir))
    
    # create cog scene directory
    if not os.path.exists(cog_scene_dir):
        print ( 'Creating scene cog directory: {}'.format(cog_scene_dir) )
        os.mkdir(cog_scene_dir)
#     else:
#         print ( 'Scene cog directory already exists so passing: {}'.format(cog_scene_dir) )
    
    
    cog_val = []
    
    des_prods = ["AOT_10m", "B01_60m", "B02_10m", "B03_10m", "B04_10m", "B05_20m", "B06_20m",
                 "B07_20m", "B08_10m", "B8A_20m", "B09_60m", "B11_20m", "B12_20m", "SCL_20m",
                 "WVP_10m"]
    
    # find all individual prods to convert to cog (ignore true colour images (TCI))
    if scene_name.split('_')[1] == 'MSIL1C':
        prod_paths = glob.glob(original_scene_dir + 'GRANULE/*/IMG_DATA/*.jp2')
    
    elif scene_name.split('_')[1] == 'MSIL2A':
        prod_paths = glob.glob(original_scene_dir + 'GRANULE/*/IMG_DATA/*/*.jp2')
        prod_paths = [x for x in prod_paths if x[-11:-4] in des_prods]
    
#     for i in prod_paths: print (i)
    
    proc_list = []
    
    # iterate over prods to create parellel processing list
    for prod in prod_paths: 
             
        in_filename = prod
        out_filename = cog_scene_dir + scene_name + prod[-12:-4] + '.tif'
                
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


def copy_s2_metadata(original_scene_dir, cog_scene_dir, scene_name):
    """
    Parse through S2 metadtaa .xml for either l1c or l2a S2 scenes.
    """
    
    if '_MSIL1C_' in original_scene_dir:
        meta_base = 'MTD_MSIL1C.xml'
    else:
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
    
    
def sen2cor_correction(sen2cor, in_dir, out_dir):
    """
    Run sen2cor on input S2 L1C product directory (must be unzipped).
    """
    cmd = '{} {} --output_dir {}'.format(sen2cor, in_dir, out_dir)
    print(cmd)
    p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    out = p.stdout.read()
    print(out)
    
    l2a_dir = glob.glob(in_dir.replace('_MSIL1C', '_MSIL2A')[:-39]+'*')[0] +'/'
    os.rename(l2a_dir, in_dir.replace('_MSIL1C_','_MSIL2A_'))

    
    
def s2_ndvi(red_file, nir_file, out_file=False):
    
    # NEED TO WRITE DIRECTLY TO COG TO DRASTICALLY SPEED UP
    
    inter = red_file[:-11] + 'NDVI_10m_inter.tif'
    
    r = rasterio.open(red_file).read(1)
    nir = rasterio.open(nir_file).read(1)
    ndvi = (nir.astype(float) - r.astype(float)) / (nir + r)
    
    with rasterio.open(red_file) as src:
        kwds = src.profile
        kwds['dtype'] = rasterio.float32
        with rasterio.open(inter, 'w', **kwds) as dst:
            dst.write(ndvi.astype(rasterio.float32), 1)
            
    if not out_file:
        out_file = red_file[:-11] + 'NDVI_10m.tif'
    
    conv_sgl_cog(inter, out_file)
    os.remove(inter)
    
    return ndvi


def s3_single_upload(in_path, s3_path, s3_bucket):
    """
    put a file into S3 from the local file system.

    :param in_path: a path to a file on the local file system
    :param s3_path: where in S3 to put the file.
    :return: None
    """
    
    # prep session & creds
    access = os.getenv("AWS_ACCESS_KEY")
    secret = os.getenv("AWS_SECRET_KEY")
    
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
    
    for i in upload_list: 
        print (s3_single_upload(i[0],i[1],i[2]))
    
    # parallelise upload
#     pool = multiprocessing.Pool(processes=5)
#     pool.starmap(s3_single_upload, upload_list)


# @click.command()
# @click.argument("in_scene")
# @click.argument("out_dir")
# @click.option("--inter_dir", default="out_dir", type=click.Path(), help="Optional intermediary directory to be used for processing. If not specified then sub-dir within out_dir is used. Ought to be specified if out_dir is Cloud Bucket.")
# @click.option("--prodlevel", default="L1C", help="Desired Sentinel-2 product level. Defaults to 'L1C'. Use 'L2A' for ARD equivalent")
# @click.option("--source", default="gcloud", help="Api source to be used for downloading scenes.")

def prepareS2(in_scene, s3_bucket='public-eo-data', s3_dir='fiji/Sentinel_2_test/', inter_dir='/data/intermediate/', prodlevel='L2A', source='gcloud'):
    """
    Prepare IN_SCENE of Sentinel-2 satellite data into OUT_DIR for ODC indexing. 

    :param in_scene: input Sentinel-2 scene name (either L1C or L2A) i.e. "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE"
    :param out_dir: output directory to drop COGs into.
    :param --inter: optional intermediary directory to be used for processing. If not specified then sub-dir within out_dir is used. Ought to be specified if out_dir is Cloud Bucket.
    :param --prodlevel: Desired Sentinel-2 product level. Defaults to 'L1C'. Use 'L2A' for ARD equivalent
    :param --source: Api source to be used for downloading scenes. Defaults to gcloud. Options inc. 'gcloud', 'esahub', 'sedas' COMING SOON
    :return: None
    
    Assumptions:
    - env set at SEN2COR_8: i.e. Sen2Cor-02.08.00-Linux64/bin/L2A_Process"
    - env set COPERNICUS_USERNAME
    - env set COPERNICUS_PWD
    - env set AWS_ACCESS
    - env set AWS_SECRET
    - maybe something to do with gcloud storage log in? Not sure if needed...
    - etc.... tbd
    """

    # Need to handle inputs with and without .SAFE extension
    if not in_scene.endswith('.SAFE'):
        in_scene = in_scene + '.SAFE'
    # shorten scene name
    scene_name = in_scene[:-21]
    scene_name = scene_name[:-17] + scene_name.split('_')[-1] 
    
    # Unique inter_dir needed for clean-up
    inter_dir = inter_dir + scene_name +'_tmp/'

    try:
#     if 'x' == 'x':
                
        # sub-dirs used only for accessing tmp files
        down_dir = inter_dir + in_scene + '/' 
        cog_dir = inter_dir + scene_name + '/'
        os.makedirs(cog_dir, exist_ok=True)
        l2a_dir = inter_dir + '/'
        
        log_file = os.path.join(inter_dir + 'log_file.csv') # Create log somewhere more sensible - assumes exists
        
        with open(log_file, 'w') as foo:
                pass
        
        with open(log_file, 'a') as log:
            
            log.write("{},{},{}".format('Scene_Name', 'Completed_Stage', 'DateTime'))
            log.write("\n")

            log.write("{},{},{}".format(in_scene, 'Start', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")

            # DOWNLOAD
            if source == "esahub":
                s2id = find_s2_uuid(in_scene)
                download_extract_s2_esa(s2id, inter_dir, down_dir)
            elif source == "gcloud":
                t = 't'
                download_s2_granule_gcloud(in_scene, inter_dir)

            log.write("{},{},{}".format(in_scene, 'Downloaded', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")

    #         # [CREATE L2A WITHIN TEMP DIRECTORY]
    #         if (scene_name.split('_')[1] == 'MSIL1C') & (prodlevel == 'L2A'):
    #             sen2cor_correction(sen2cor8, down_dir, l2a_dir)

            # CONVERT TO COGS TO TEMP COG DIRECTORY**
            conv_s2scene_cogs(down_dir, cog_dir, scene_name)

            log.write("{},{},{}".format(in_scene, 'COGS', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")

            # PARSE METADATA TO TEMP COG DIRECTORY**
            copy_s2_metadata(down_dir, cog_dir, scene_name) 

            # GENERATE YAML WITHIN TEMP COG DIRECTORY**
            create_yaml(cog_dir, 's2')

            log.write("{},{},{}".format(in_scene, 'Yaml', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")        
        
            # AMEND SO THAT LOG INCCLUDES UPLOAD TIME BEFORE BEING UPLOADED ITSELF....
            # MOVE COG DIRECTORY TO OUTPUT DIRECTORY
            s3_upload_cogs(glob.glob(cog_dir + '*'), s3_bucket, s3_dir)

            log.write("{},{},{}".format(in_scene, 'Uploaded', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")        
            
        shutil.move(log_file, cog_dir + 'log_file.csv')
        s3_upload_cogs(glob.glob(cog_dir + '*.csv'), s3_bucket, s3_dir)
        
                
        # DELETE ANYTHING WITIN TEH TEMP DIRECTORY
        cmd = 'rm -frv {}'.format(inter_dir)
        p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()
        
    
    except:
        
        # DELETE ANYTHING WITIN TEH TEMP DIRECTORY
        cmd = 'rm -frv {}'.format(inter_dir)
        p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()
        print("Something didn't work!")


# if __name__ == '__main__':

#     prepareS2()

