from datetime import datetime
from dateutil import parser
from dateutil.parser import parse
import glob
import os
import rasterio
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from rasterio.shutil import copy
from osgeo import osr
import uuid
from pathlib import Path
from xml.etree import ElementTree  # should use cElementTree..
import yaml
import boto3



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

#     print(prod_name.split('_'))
    if prod_name.split('_')[1] == 'MSIL1C':
        print(prod_name)
        prod_name = prod_name.split('_')[-1][:-4]
        prod_map = {
            "B01": 'coastal_aerosol',
            "B02": 'blue',
            "B03": 'green',
            "B04": 'red',
            "B05": 'vegetation_red_edge_1',
            "B06": 'vegetation_red_edge_2',
            "B07": 'vegetation_red_edge_3',
            "B08": 'nir',
            "B8A": 'water_vapour',
            "B09": 'swir_1',
            "B10": 'swir_cirrus',
            "B11": 'swir_2',
            "B12": 'narrow_nir',
            "TCI": 'true_colour'       
        }   
        
    else:
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
    
    prod_paths = glob.glob(scene_dir + '*.tif')
        
    t0=parse(str( datetime.strptime(prod_paths[0].split("_")[-3], '%Y%m%dT%H%M%S')))
    print ( t0 )
    t1=t0
    print ( t1 )

    # get polorisation from each image product (S1 band)
    # should be replaced with a more concise, generalisable parsing
    images = {
        band_name_s1(prod_path): {
            'path': str(prod_path.split('/')[-1])
        } for prod_path in prod_paths
    }
    print ( images )
    
    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry('/'.join([str(scene_dir), images['vh']['path']]))
    
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
    
    if scene_dir.split('/')[-2].split('_')[1] == 'MSIL1C':
        t0=parse(str( datetime.strptime(prod_paths[0].split("_")[-3], '%Y%m%dT%H%M%S')))
    else:
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
#     print ( images )
    
    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry('/'.join([str(scene_dir), images['blue']['path']]))
    
    # parse esa l2a prod metadata file for reference
    scene_genesis =  glob.glob(scene_dir + '*MTD_*.xml')[0]
    if os.path.exists(scene_genesis):
        scene_genesis = os.path.basename(scene_genesis)
    else:
        scene_genesis = ' '
    
    new_id = str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name))
#     print ('New uuid: {}'.format(new_id))
    
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
        metadata = yaml_prep_s1(scene_dir)

    elif sensor == 's2':
        metadata = yaml_prep_s2(scene_dir)
                        
    yaml_path = str(scene_dir + 'datacube-metadata.yaml')
    
    # not sure why default_flow_style is now required - strange...
    with open(yaml_path, 'w') as stream:
        yaml.dump(metadata, stream, default_flow_style=False)
        
    print ( 'Created yaml: {}'.format(yaml_path) )
        


def s3_single_upload(in_path, s3_path, s3_bucket):
    """
    put a file into S3 from the local file system.

    :param in_path: a path to a file on the local file system
    :param s3_path: where in S3 to put the file.
    :return: None
    """
    
    # prep session & creds
    access = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    
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
