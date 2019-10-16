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

try:
    from .prep_utils import *
except:
    from prep_utils import *
    

def download_s2_granule_gcloud(s2_id, download_dir, safe_form=True, bands=False):
    """
    Downloads a single Sentinel-2 (L1C or L2A) acquisition from GCloud bucket into new S2ID directory

    :param s2_id: ID for Sentinel-2 Granule (i.e. "S2B_MSIL1C_20190815T110629_N0208_R137_T30UWB_20190815T135651")
    :param download_dir: path to dir for downloaded S2 granule dir to be created within (doesn't have to already exist)
    :param safe_form: download into .SAFE folder structure or single dir of .jp2s. default=True
    :param bands: download only a subset of S2 bands. default is False. input is list i.e. ["B02.jp2", "B03.jp2"]
    :return:
    """
    if s2_id.endswith('.SAFE'):
        s2_id = s2_id[:-5]
    
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name="gcp-public-data-sentinel-2", user_project=None)
    
    dir_name = download_dir
    if (not safe_form) & (not os.path.exists(dir_name)):
        os.makedirs(dir_name)

    identifiers = s2_id.split('_')[5]
    dir1 = identifiers[1:3]
    dir2 = identifiers[3]
    dir3 = identifiers[4:6]
    
    if 'MSIL1C' in s2_id:
        prefix = "tiles/%s/%s/%s/%s.SAFE" % (str(dir1), str(dir2), str(dir3), str(s2_id))
    elif 'MSIL2A' in s2_id:
        prefix = "L2/tiles/%s/%s/%s/%s.SAFE" % (str(dir1), str(dir2), str(dir3), str(s2_id))
    blobs = bucket.list_blobs(prefix=prefix)  # Get list of files
    
    # filter bands if needed
    if bands:
        try:
            des_blobs = []
            for blob in blobs:
                if blob.name[-7:] in bands:
                    des_blobs.append(blob)
            blobs = des_blobs
        except:
            print('Bands either False or list. I.e. [B02.jp2, B03.jp2]')
    
    for blob in blobs:
        if (not blob.name.endswith("$")): # weird end directory signifier...

            if not safe_form:
                name = os.path.join(dir_name, os.path.basename(blob.name))

            else:
                interdir = os.path.join(dir_name,'/'.join(blob.name.split('/')[5:-1]))

                if not os.path.exists(interdir):
                    os.makedirs(interdir)
                name = os.path.join(dir_name + '/'.join(blob.name.split('/')[5:]))

            blob.download_to_filename(name)

            
def find_s2_uuid(s2_filename):
    """
    Returns S2 uuid required for download via sentinelsat, based upon an input S2 file/scene name. 
    I.e. S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410
    Assumes esa hub creds stored as env variables.
    
    :param s2_file_name: Sentinel-2 scene name
    :return s2_uuid: download id
    """
    copernicus_username=os.getenv("COPERNICUS_USERNAME")
    copernicus_pwd=os.getenv("COPERNICUS_PWD")
    print(f"ESA username: {copernicus_username}")
    esa_api = SentinelAPI(copernicus_username, copernicus_pwd)
    
    if s2_filename[-5:] == '.SAFE':
        res = esa_api.query(filename=s2_filename)
        res = esa_api.to_geodataframe(res)
        
        return res.uuid.values[0]


def download_extract_s2_esa(scene_uuid, down_dir, original_scene_dir):
    """
    Download a single S2 scene from ESA via sentinelsat 
    based upon uuid. 
    Assumes esa hub creds stored as env variables.
    
    :param scene_uuid: S2 download uuid from sentinelsat query
    :param down_dir: directory in which to create a downloaded product dir
    :param original_scene_dir: 
    :return: 
    """
    # if unzipped .SAFE file doesn't exist then we must do something
    if not os.path.exists(original_scene_dir):
        
        # if downloaded .zip file doesn't exist then download it
        if not os.path.exists(original_scene_dir.replace('.SAFE/','.zip')):
            print ( 'Downloading ESA scene zip: {}'.format(os.path.basename(original_scene_dir)) )

            copernicus_username=os.getenv("COPERNICUS_USERNAME")
            copernicus_pwd=os.getenv("COPERNICUS_PWD")
            print(f"ESA username: {copernicus_username}")
            esa_api = SentinelAPI(copernicus_username, copernicus_pwd)
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
    Convert S2 scene products to cogs [+ validate TBC].
    Works for both L1C and L2A .SAFE dir structures.
    
    :param original_scene_dir: Downloaded S2 product directory (i.e. via ESA or GCloud; assumes .SAFE structure) 
    :param cog_scene_dir: directory in which to create the output COGs
    :param scene_name: shortened S2 scene name (i.e. S2A_MSIL2A_20190124T221941_T60KYF from S2A_MSIL2A_20190124T221941_N0211_R029_T60KYF_20190124T234344)
    :param overwrite: Binary for whether to overwrite or skip existing COG files)
    :return: 
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


def copy_s2_metadata(original_scene_dir, cog_scene_dir, scene_name):
    """
    Parse through S2 metadtaa .xml for either l1c or l2a S2 scenes.
    
    :param original_scene_dir: downloaded S2 dir in which to find original metadata (MTD_*.xml) file.
    :param cog_scene_dir: dir in which to copy MTD into
    :param scene_name: shortened S2 scene name (i.e. S2A_MSIL2A_20190124T221941_T60KYF from S2A_MSIL2A_20190124T221941_N0211_R029_T60KYF_20190124T234344)
    :return: 
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
    
    :param sen2cor: path to sen2cor run file (i.e. ~/Sen2Cor-02.08.00-Linux64/bin/L2A_Process)
    :param in_dir: input L1C S2 .SAFE scene dir
    :param out_dir: output dir in which to create a .SAFE L2A product dir
    :return: 
    """
    cmd = '{} {} --output_dir {}'.format(sen2cor, in_dir, out_dir)
    print(cmd)
    p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    out = p.stdout.read()
    print(out)
#     retVal = p.Wait()
#     if retVal != 0:
#         print("oh no it went wrong") 
    try:
        l2a_dir = glob.glob(in_dir.replace('_MSIL1C', '_MSIL2A')[:-39]+'*')[0] +'/'
        os.rename(l2a_dir, in_dir.replace('_MSIL1C_','_MSIL2A_'))
    except:
        raise Exception(out)
        

def s2_ndvi(red_file, nir_file, out_file=False):
    """
    Not useful atm.
    """
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


# @click.command()
# @click.argument("in_scene")
# @click.argument("out_dir")
# @click.option("--inter_dir", default="out_dir", type=click.Path(), help="Optional intermediary directory to be used for processing. If not specified then sub-dir within out_dir is used. Ought to be specified if out_dir is Cloud Bucket.")
# @click.option("--prodlevel", default="L1C", help="Desired Sentinel-2 product level. Defaults to 'L1C'. Use 'L2A' for ARD equivalent")
# @click.option("--source", default="gcloud", help="Api source to be used for downloading scenes.")

def prepareS2(in_scene, s3_bucket='public-eo-data', s3_dir='fiji/Sentinel_2_test/', inter_dir='/tmp/data/intermediate/', prodlevel='L2A'):
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
    # Need to handle inputs with and without .SAFE extension
    if not in_scene.endswith('.SAFE'):
        in_scene = in_scene + '.SAFE'
    # shorten scene name
    scene_name = in_scene[:-21]
    scene_name = scene_name[:-17] + scene_name.split('_')[-1] 
    
    sen2cor8 = os.environ.get("SEN2COR_8")
    
    # Unique inter_dir needed for clean-up
    inter_dir = inter_dir + scene_name +'_tmp/'
    os.makedirs(inter_dir, exist_ok=True)
    # sub-dirs used only for accessing tmp files
    down_dir = inter_dir + in_scene + '/' 
    cog_dir = inter_dir + scene_name + '/'
    os.makedirs(cog_dir, exist_ok=True)
    l2a_dir = inter_dir + '/'    
    
    # Logging structure taken from - https://www.loggly.com/ultimate-guide/python-logging-basics/
    log_file = inter_dir+'log_file.txt'
    handler = logging.handlers.WatchedFileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(os.environ.get("LOGLEVEL", "DEBUG"))
    root.addHandler(handler)
    
    root.info(f"{in_scene} {scene_name} Starting")
    
    try:
        
        # DOWNLOAD
        try:
            root.info(f"{in_scene} {scene_name} DOWNLOADING via GCloud")
            download_s2_granule_gcloud(in_scene, inter_dir)
            root.info(f"{in_scene} {scene_name} DOWNLOADED via GCloud")
        except:
            root.exception(f"{in_scene} {scene_name} UNAVAILABLE via GCloud, try ESA")
            try:
                s2id = find_s2_uuid(in_scene)
                print(s2id)
                root.info(f"{in_scene} {scene_name} AVAILABLE via ESA")
                download_extract_s2_esa(s2id, inter_dir, down_dir)
                root.info(f"{in_scene} {scene_name} DOWNLOADED via ESA")
            except:
                root.exception(f"{in_scene} {scene_name} UNAVAILABLE via ESA too")
                raise Exception('Dwonload Error ESA')
            
        # [CREATE L2A WITHIN TEMP DIRECTORY]
        if (scene_name.split('_')[1] == 'MSIL1C') & (prodlevel == 'L2A'):
            root.info(f"{in_scene} {scene_name} Sen2Cor Processing")
            try:
                sen2cor_correction(sen2cor8, down_dir, l2a_dir)
                root.info(f"{in_scene} {scene_name} Sen2Cor COMPLETE")
            except:
                root.exception(f"{in_scene} {scene_name} sen2cor FAILED")
                raise Exception('Sen2Cor Error')
        
        # CONVERT TO COGS TO TEMP COG DIRECTORY**
        try:
            root.info(f"{in_scene} {scene_name} Converting COGs")
            conv_s2scene_cogs(down_dir, cog_dir, scene_name)
            root.info(f"{in_scene} {scene_name} COGGED")
        except:
            root.exception(f"{in_scene} {scene_name} COG conversion FAILED")
            raise Exception('COG Error')        
        
        # PARSE METADATA TO TEMP COG DIRECTORY**
        try:
            root.info(f"{in_scene} {scene_name} Copying original METADATA")
            copy_s2_metadata(down_dir, cog_dir, scene_name) 
            root.info(f"{in_scene} {scene_name} COPIED original METADATA")
        except:
            root.exception(f"{in_scene} {scene_name} MTD not coppied")
        
        # GENERATE YAML WITHIN TEMP COG DIRECTORY**
        try:
            root.info(f"{in_scene} {scene_name} Creating dataset YAML")
            create_yaml(cog_dir, 's2')
            root.info(f"{in_scene} {scene_name} Created original METADATA")
        except:
            root.exception(f"{in_scene} {scene_name} Dataset YAML not created")
            raise Exception('YAML creation error')        

        # MOVE COG DIRECTORY TO OUTPUT DIRECTORY
        try:
            root.info(f"{in_scene} {scene_name} Uploading to S3 Bucket")
            s3_upload_cogs(glob.glob(cog_dir + '*'), s3_bucket, s3_dir)
            root.info(f"{in_scene} {scene_name} Uploaded to S3 Bucket")
        except:
            root.exception(f"{in_scene} {scene_name} Upload to S3 Failed")
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


# if __name__ == '__main__':

#     prepareS2()

