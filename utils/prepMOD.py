import tarfile
import uuid
import requests
import glob
import os
import logging
from dateutil.parser import parse
from datetime import datetime
from datetime import timedelta
from subprocess import Popen, PIPE, STDOUT

from utils.prep_utils import *


def download_modis(scene_id, down_path):
    """
    Download a MCD43A4 scene/entity ID from LAADS DAAC into filepath.
    Assumes 'LAADSDAAC_KEY' env var is set.
    """
    if not os.path.exists(down_path):
        down_dir = f"{os.path.dirname(down_path)}/"
        os.makedirs(down_dir, exist_ok=True)

        app_key = os.environ['LAADSDAAC_KEY']

        base_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/6/MCD43A4/"    
        yr = scene_id.split(".")[1][1:5]
        dy = scene_id.split(".")[1][5:8]
        url = f"{base_url}{yr}/{dy}/{scene_id}"
        logging.info(f"Download url {url}")
        
        cmd = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=6 "{url}" --header "Authorization: Bearer {app_key}" -P {down_dir}'
        logging.info(cmd)
        p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        out = p.stdout.read()
        logging.info(out)

    else:
        logging.info(f"Scene already downloaded: {down_path}")

        
def modis_hdf2cogs(hdf_path, cog_dir):
    """
    Convert MCD43A4 HDF if subdatasets into individual COGs.
    """
    scene_name = cog_dir.split('/')[-2]
    
    with rasterio.open(hdf_path) as src:
        prod_paths = src.subdatasets
    prod_names = [i.split(':')[-1] for i in prod_paths]

    for non_cog, prod_name in zip(prod_paths, prod_names):
        cog = os.path.join(cog_dir,f"{scene_name}_{prod_name}.tif")
        conv_sgl_cog(non_cog, cog)
        logging.info(f'cogged: {non_cog} {cog}')
        
def band_name_MCD43A4(prod_path):
    """
    Determine MCD43A4 band of individual product from product name
    from path to specific product file
    """
    prod_name = os.path.basename(prod_path)[24:-4]
    logging.debug("Product name is: {}".format(prod_name))

    prod_map = {
        "Nadir_Reflectance_Band1": 'red',
        "Nadir_Reflectance_Band2": 'nir',
        "Nadir_Reflectance_Band3": 'blue',
        "Nadir_Reflectance_Band4": 'green',
        "Nadir_Reflectance_Band5": 'swir1',
        "Nadir_Reflectance_Band6": 'swir2',
        "Nadir_Reflectance_Band7": 'mwir',
        "BRDF_Albedo_Band_Mandatory_Quality_Band1": 'qa_red',
        "BRDF_Albedo_Band_Mandatory_Quality_Band2": 'qa_nir',
        "BRDF_Albedo_Band_Mandatory_Quality_Band3": 'qa_blue',
        "BRDF_Albedo_Band_Mandatory_Quality_Band4": 'qa_green',
        "BRDF_Albedo_Band_Mandatory_Quality_Band5": 'qa_swir1',
        "BRDF_Albedo_Band_Mandatory_Quality_Band6": 'qa_swir2',
        "BRDF_Albedo_Band_Mandatory_Quality_Band7": 'qa_mwir'    }
    
    layer_name = prod_map[prod_name]

    return layer_name



def find_MCD43A4_datetime(scene_dir):
    """
    Create datetime from MCD43A4 cog scene dir.
    Midday used for time as composite of terrra+acqua.
    """
    yr = int(scene_dir.split('/')[-2].split('_')[1][1:5])
    dy = int(scene_dir.split('/')[-2].split('_')[1][5:8])    
    return str(datetime(yr, 1, 1) + timedelta(dy - 1) + timedelta(hours=12))
    
    
def yaml_prep_MCD43A4(scene_dir):
    """
    Prepare individual MODIS scene directory containing MCD43A4 cog products converted
    from USGS-generated MCD43A4 L3 products.
    """
    # scene_name = scene_dir.split('/')[-2][:26]
    scene_name = scene_dir.split('/')[-2]
    logging.info(f"Preparing scene {scene_name}")
    logging.info(f"Scene path {scene_dir}")

    prod_paths = glob.glob(f"{scene_dir}*.tif")
    logging.info(prod_paths)

    t0 = parse(find_MCD43A4_datetime(scene_dir))
    
    # get dc name and path for each image band
    images = {
        band_name_MCD43A4(prod_path): {
            'path': str(os.path.basename(prod_path))
        } for prod_path in prod_paths
    }
    logging.info(images)
    
    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry(os.path.join(str(scene_dir), images['blue']['path']))
    
    scene_genesis = ' ' # nothing to inc.
    
    new_id = str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name))
    platform_code = ""
    instrument_name = ""
    if "MCD43A4" in scene_name:
        logging.info(f"{scene_name} detected as landsat 8")
        platform_code = "MODIS_NBAR"
        instrument_name = "MODIS"
    else:
        raise Exception(f"Unknown platform {scene_name}")

    return {
        'id': new_id,
        'processing_level': "usgsl3_cog",
        'product_type': "optical_ard",
        'creation_dt': str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')),
        'platform': {
            'code': platform_code
        },
        'instrument': {
            'name': instrument_name
        },
        'extent': create_metadata_extent(extent, t0, t0),
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


def prepareMOD(in_scene, 
               s3_bucket='public-eo-data', 
               s3_dir='common_sensing/fiji/default',
               inter_dir='/tmp/data/intermediate/'):
    
    root = setup_logging()

    scene_name = '_'.join(in_scene.split('/')[-1].replace('.','_').split('_')[0:3])
    inter_dir = "/tmp/data/intermediate/"
    os.makedirs(inter_dir, exist_ok=True)
    down_path = os.path.join(inter_dir, in_scene)
    cog_dir = f"{inter_dir}{scene_name}/"
    os.makedirs(cog_dir,exist_ok=True)
    
    logging.info(f"scene: {in_scene}\ndownload: {down_path}\ncog_dir: {cog_dir}")
    root.info(f"{scene_name} Starting")

    try:
        
        try:
            root.info(f"{scene_name} DOWNLOADING via LAADSDAAC")
            download_modis(in_scene, down_path)
            root.info(f"{scene_name} DOWNLOADED")
        except Exception as e:
            root.exception(f"{scene_name} CANNOT BE FOUND")
            raise Exception('Download Error', e)

        try:
            root.info(f"{scene_name} Converting COGs")
            modis_hdf2cogs(down_path, cog_dir)
            root.info(f"{scene_name} COGGED")
        except Exception as e:
            root.exception(f"{scene_name} CANNOT BE COGGED")
            raise Exception('COG Error', e)

        try:
            root.info(f"{scene_name} Creating yaml")
            create_yaml(cog_dir, yaml_prep_MCD43A4(cog_dir))
            root.info(f"{scene_name} Created yaml")
        except Exception as e:
            root.exception(f"{scene_name} yaml not created {e}")
            raise Exception('Yaml error', e)

        try:
            root.info(f"{scene_name} Uploading to S3 Bucket")
            s3_upload_cogs(glob.glob(cog_dir + '*'), s3_bucket, s3_dir)
            root.info(f"{scene_name} Uploaded to S3 Bucket")
        except Exception as e:
            root.exception(f"{scene_name} Upload to S3 Failed")
            raise Exception('S3  upload error', e)

        clean_up(inter_dir)

    except Exception as e:
        logging.error(f"Could not process {scene_name}, {e}")
#         clean_up(inter_dir)


if __name__ == '__main__':
    
    prepareMOD("MCD43A4.A2020008.h00v08.006.2020017034128.hdf")