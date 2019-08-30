from datetime import datetime
from dateutil import parser
from dateutil.parser import parse
import glob
import os
import rasterio
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from rasterio import shutil
from rasterio.shutil import copy
from osgeo import osr
import uuid
from pathlib import Path
from xml.etree import ElementTree  # should use cElementTree..
import yaml

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
        
        