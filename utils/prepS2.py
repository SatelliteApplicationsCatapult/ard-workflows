import os
import time
from tqdm import tqdm
from google.cloud import storage
import glob
from sentinelsat import SentinelAPI
import zipfile
import shutil

from .cogeo import *


def find_s2_uuid(s2_filename):
    """
    Finds uuid required for download based upon an input S2 file/scene name. 
    I.e. S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410
    """
    esa_api = SentinelAPI('tmj21','Welcome12!')
    if s2_filename[-5:] == '.SAFE':
        res = esa_api.query(filename=s2_filename)
        res = esa_api.to_geodataframe(res)
    else:
        res = esa_api.query(filename=s2_filename+'.SAFE')
        res = esa_api.to_geodataframe(res)
    return res.uuid.values[0]


def download_s2_granule_gcloud(s2_id, download_dir, safe_form=True, bands=False):
    """
    Downloads RGBNIR bands of single Sentinel-2 acquisition from GCloud bucket into new S2ID directory

    :param s2_id: ID for Sentinel Tile (i.e. "S2B_MSIL1C_20190815T110629_N0208_R137_T30UWB_20190815T135651")
    :param download_dir: path to directory for downloaded S2 granules
    :param bucket: GCloud bucket object containing all Sentinel imagery
    :param safe_form: download into .SAFE folder structure. default=True
    :param bands: download only a subset of S2 bands. default is False. input is list i.e. [B02.jp2, B03.jp2]
    :return:
    
    TO DO:UPDATE PARAMS
    """

    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name="gcp-public-data-sentinel-2", user_project=None)
    
    dir_name = os.path.join(download_dir, s2_id)
    if (not safe_form) & (not os.path.exists(dir_name)):
        os.makedirs(dir_name)

    identifiers = s2_id.split('_')[5]
    dir1 = identifiers[1:3]
    dir2 = identifiers[3]
    dir3 = identifiers[4:6]
    
    prefix = "tiles/%s/%s/%s/%s.SAFE" % (str(dir1), str(dir2), str(dir3), str(s2_id))
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
                interdir = os.path.join(dir_name +'.SAFE', '/'.join(blob.name.split('/')[5:-1]))

                if not os.path.exists(interdir):
                    os.makedirs(interdir)

                name = os.path.join(dir_name +'.SAFE', '/'.join(blob.name.split('/')[5:]))
            blob.download_to_filename(name)

            
            
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
    prod_paths = glob.glob(original_scene_dir + 'GRANULE/*/IMG_DATA/*/*.jp2')
    prod_paths = [x for x in prod_paths if x[-11:-4] in des_prods]
    
#     print(prod_paths)
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
                
                conv_sgl_cog( in_filename, out_filename )
                
            else:
                print ( 'cog already exists: {}'.format(out_filename) )
        else:
            print ( 'cannot find product: {}'.format(in_filename) )
            
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
    if ds is None:
        print ('not updated nodata')

    b = ds.GetRasterBand(1)
    b.SetNoDataValue(0)
    b.FlushCache()
    b = None
    ds = None 


    
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
    cmd = sen2cor + ' ' + '--output_dir ' + out_dir + ' ' + in_dir
    print(cmd)
    os.system(cmd)

    l2a_dir = glob.glob(in_dir.replace('_MSIL1C', '_MSIL2A')[:-39]+'*')[0] +'/'
    os.rename(l2a_dir, in_dir.replace('_MSIL1C_','_MSIL2A_'))

                
# def download_granules(s2_ids, download_dir, safe_form=True, bands=False):
#     """
#     Downloads RGBNIR bands of Sentinel-2 acquisitions from GCloud bucket into new S2ID directories

#     :param s2_ids: List of Sentinel-2 IDs (i.e. "S2B_MSIL1C_20190815T110629_N0208_R137_T30UWB_20190815T135651")
#     :param download_dir: path to directory for downloaded S2 granules
#     :param safe_form: download into .SAFE folder structure. default=True
#     :param bands: download only a subset of S2 bands. default is False. input is list i.e. [B02.jp2, B03.jp2]
#     """
    
#     client = storage.Client.create_anonymous_client()
#     bucket = client.bucket(bucket_name="gcp-public-data-sentinel-2", user_project=None)
    
#     for s2_id in tqdm(s2_ids[:]):
#         print("Attempting to download: {}".format(s2_id))
#         start = time.time()
#         download_one_granule(s2_id, download_dir, bucket, safe_form=safe_form, bands=bands)
#         end = time.time()
#         print("Downloaded: {}".format(end - start))

