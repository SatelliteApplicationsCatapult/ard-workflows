import yaml
import glob
import rasterio
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

from . dc_water_classifier import wofs_classify
from . dc_clean_mask import landsat_qa_clean_mask
from . prep_utils import s3_list_objects, s3_download, s3_upload_cogs, create_yaml, cog_translate
from . dc_import_export import export_xarray_to_geotiff


    
def rename_bands(in_xr, des_bands, position):
    in_xr.name = des_bands[position]
    return in_xr
    
    
def conv_sgl_wofs_cog(in_path, out_path, nodata=-9999):
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
    
#     ds = gdal.Open(in_path, gdal.GA_Update)
#     if ds is not None:
#         b = ds.GetRasterBand(1)
#         b.SetNoDataValue(nodata)
#         b.FlushCache()
#         b = None
#         ds = None 
#     else:
#         print ('not updated nodata')


def yaml_prep_wofs(scene_dir, original_yml):
    """
    Prepare individual wofs directory containing L8/S2/S1 cog water products.
    """
    # scene_name = scene_dir.split('/')[-2][:26]
    scene_name = scene_dir.split('/')[-2]
    print ( "Preparing scene {}".format(scene_name) )
    print ( "Scene path {}".format(scene_dir) )
    
    # find all cog prods
    prod_paths = glob.glob(scene_dir + '*water.tif')
    # print ( 'paths: {}'.format(prod_paths) )
    # for i in prod_paths: print ( i )
    
    # date time assumed eqv for start and stop - this isn't true and could be 
    # pulled from .xml file (or scene dir) not done yet for sake of progression
    t0=parse(str(datetime.strptime(original_yml['extent']['center_dt'], '%Y-%m-%d %H:%M:%S')))
    # print ( t0 )
    t1=t0
    # print ( t1 )
    
    # get polorisation from each image product (S2 band)
#     images = {
#         band_name_l8(prod_path): {
#             'path': str(prod_path.split('/')[-1])
#         } for prod_path in prod_paths
#     }

    images = { 'water': { 'path': str(prod_paths[0].split('/')[-1]) } }
    
    #     print ( images )
    
    # trusting bands coaligned, use one to generate spatial bounds for all
#     projection, extent = get_geometry('/'.join([str(scene_dir), images['blue']['path']]))
    
    # parse esa l2a prod metadata file for reference
#     scene_genesis =  glob.glob(scene_dir + '*.xml')[0]
#     if os.path.exists(scene_genesis):
#         scene_genesis = os.path.basename(scene_genesis)
#     else:
#         scene_genesis = ' '
    
    new_id = str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name))
#     print ('New uuid: {}'.format(new_id))
    
    return {
        'id': new_id,
        'processing_level': original_yml['processing_level'],
        'product_type': "water",
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
            'projection': original_yml['grid_spatial']['projection']
        },
        'image': {
            'bands': images
        },
        'lineage': {
            'source_datasets': original_yml['lineage']['source_datasets'],
        }  

    }
    
    
def resamp_bands(xr, xrs):
    if xr.attrs['res'] == xrs[0].attrs['res']:
        return xr
    else:
        return xr.interp(x=xrs[0]['x'], y=xrs[0]['y'])
    
    
def per_scene_wofs(optical_yaml_path, s3_source=True, s3_bucket='public-eo-data', s3_dir='common_sensing/fiji/wofsdefault/', inter_dir='../tmp/data/intermediate/', aoi_mask=False):
    """
    Generate and prepare wofs (and wofs-like) products for .
    Assumes all data can be found and downoaded using relative locations within yaml & dir name contains unique scene_name.
    
    To do:
    - inc. wofl as opposed to just wofs
    - generalise to s2 - should be easy
    - generalise to s1 - need to coalesce on approach (basic thresholding initially?)
    - check acknowledgements
    """
    # Assume dirname of yml references name of the scene - should hold true for all ard-workflows prepared scenes
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
    aoi = f'{inter_dir}mask_aoi.geojson'
    
    des_band_refs = {
        "LANDSAT_8": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_7": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_5": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "LANDSAT_4": ['blue','green','red','nir','swir1','swir2','pixel_qa'],
        "SENTINEL_2": ['blue','green','red','nir','swir1','swir2','scene_classification'],
        "SENTINEL_1": ['VV','VH','somethinglayover shadow']}
    
    try:
        
        try:
            root.info(f"{scene_name} Finding & Downloading yml & data")
            # load yml plus download any needed files
            if (s3_source) & (not os.path.exists(yml)):
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
            else:
                print('boo')
            if aoi_mask:
                s3_download(s3_bucket, aoi_mask, aoi)
            else:
                aoi = False 
            root.info(f"{scene_name} Found & Downloaded yml & data")
        except:
            root.exception(f"{scene_name} Yaml or band files can't be found")
            raise Exception('Download Error')
    
        try:
            root.info(f"{scene_name} Loading & Reformatting bands")
            # data loading pre-requisite xarray format for applying mask + wofs classifier
#             o_bands_data = [ xr.open_rasterio(inter_dir + yml_meta['image']['bands'][b]['path'], chunks={'band': 1, 'x': 1024, 'y': 1024}) for b in des_bands ] # dask can't be used here due to resample req
            o_bands_data = [ xr.open_rasterio(inter_dir + yml_meta['image']['bands'][b]['path']) for b in des_bands ] # loading
            o_bands_data = [ resamp_bands(i, o_bands_data) for i in o_bands_data ]
            bands_data = xr.merge([rename_bands(bd, des_bands, i) for i,bd in enumerate(o_bands_data)]).rename({'band': 'time'}) # ensure band names & dims consistent
            bands_data = bands_data.assign_attrs(o_bands_data[0].attrs) # crs etc. needed later
            bands_data['time'] = [datetime.strptime(yml_meta['extent']['center_dt'], '%Y-%m-%d %H:%M:%S')] # time dim needed for wofs
            root.info(f"{scene_name} Loaded & Reformatted bands")
        except:
            root.exception(f"{scene_name} Band data not loaded properly")
            raise Exception('Data formatting error')

        try:
            root.info(f"{scene_name} Applying masks")
            # if landsat in satellite:
            if 'LANDSAT' in satellite:
                clearsky_masks = landsat_qa_clean_mask(bands_data, satellite) # easy amendment in this function to inc. sentinel-2...?
            elif 'SENTINEL_2' in satellite:
                clearsky_masks = (
                    (bands_data.scene_classification == 2) | # DARK_AREA_PIXELS
                    (bands_data.scene_classification == 4) | # VEGETATION
                    (bands_data.scene_classification == 5) | # NON_VEGETATION
                    (bands_data.scene_classification == 6) | # WATER
                    (bands_data.scene_classification == 7)   # UNCLASSIFIED
                )
            else:
                raise Exception('clearsky masking not possible')
            # elif sentinel-1 in satellite:
#             clearsky_masks = landsat_qa_clean_mask(bands_data, satellite) # easy amendment in this function to inc. sentinel-2...?
            
            clearsky_scenes = bands_data.where(clearsky_masks)
#             if satellite == 'SENTINEL_2':
#                 clearsky_scenes = clearsky_scenes.rename_vars({'swir_1': 'swir1', 'swir_2': 'swir2'})
            root.info(f"{scene_name} Loading & Reformatting bands")
        except:
            root.exception(f"{scene_name} Masks not applied")
            raise Exception('Data formatting error')

        try:
            root.info(f"{scene_name} Water classification")
            water_classes = wofs_classify(clearsky_scenes, no_data = np.nan , x_coord='x', y_coord = "y") # will work for s2 if eqv bands formatted
#             water_classes = woffles(clearsky_scenes) # will work for s2 if eqv bands formatted
            
            # TO DO - add extra line to apply S1 classifier 
            if aoi_mask:
                water_classes.attrs['crs'] = clearsky_scenes.attrs['crs']
                water_classes.attrs['transform'] = clearsky_scenes.attrs['transform']
                shp = gpd.read_file(aoi).to_crs(water_classes.attrs['crs'])
                mask = rasterio.features.rasterize(((feature['geometry'], 1) for feature in shp.iterfeatures()),
                                                   out_shape=water_classes.isel(time=0).wofs.shape,
                                                   fill=0,
                                                   transform=clearsky_scenes.transform
                                                  )
                mask = xr.DataArray(mask, coords=(water_classes.y, water_classes.x))
                water_classes = water_classes.where(clearsky_masks).where(mask) # re-apply nan mask to differentiate no-water from no-data
                print('mask worked')
            else:
                water_classes = water_classes.where(clearsky_masks) # re-apply nan mask to differentiate no-water from no-data
            water_classes = water_classes.fillna(-9999) # -9999 
            water_classes = water_classes.squeeze('time') # can't write geotif with time dim
            water_classes['wofs'] = water_classes['wofs'].astype('int16') # save space by changing type from float64
            root.info(f"{scene_name} Water classified")
        except:
            root.exception(f"{scene_name} Water classification failed")
            raise Exception('Classification error')        

        try:
            root.info(f"{scene_name} Exporting water product")            
            dataset_to_output = water_classes
            if 'MSIL2A' in inter_dir:
                output_file_name = f'{inter_dir}{"_".join(yml_meta["image"]["bands"]["blue"]["path"].split("_")[:4])}_waternc.tif' # can't
            else:
                output_file_name = f'{inter_dir}{"_".join(yml_meta["image"]["bands"]["blue"]["path"].split("_")[:7])}_waternc.tif' # can't write directly to cog...(?)
            export_xarray_to_geotiff(dataset_to_output, output_file_name, x_coord='x', y_coord='y', crs=bands_data.attrs['crs'])
            if 'MSIL2A' in inter_dir:
                output_cog_name = f'{cog_dir}{"_".join(yml_meta["image"]["bands"]["blue"]["path"].split("_")[:4])}_water.tif'
            else:
                output_cog_name = f'{cog_dir}{"_".join(yml_meta["image"]["bands"]["blue"]["path"].split("_")[:7])}_water.tif'
            conv_sgl_wofs_cog(output_file_name, output_cog_name)
            root.info(f"{scene_name} Exported COG water product")
        except:
            root.exception(f"{scene_name} Water product export failed")
            raise Exception('Export error')
            
        try:
            root.info(f"{scene_name} Creating yaml")
            create_yaml(cog_dir, yaml_prep_wofs(cog_dir, yml_meta)) # assumes majority of meta copied from original product yml
            root.info(f"{scene_name} Created yaml")
        except:
            root.exception(f"{scene_name} yam not created")
            raise Exception('Yaml error')

        try:
            root.info(f"{scene_name} Uploading to S3 Bucket")
            s3_upload_cogs(glob.glob(f'{cog_dir}*'), s3_bucket, s3_dir)
            root.info(f"{scene_name} Uploaded to S3 Bucket")
        except:
            root.exception(f"{scene_name} Upload to S3 Failed")
            raise Exception('S3  upload error')

        root.removeHandler(handler)
        handler.close()
        
        for i in o_bands_data: i.close()
        bands_data.close()
        clearsky_masks.close()
        clearsky_scenes.close()
        water_classes.close()
        dataset_to_output.close()
        
#         # Tidy up log file to ensure upload
#         shutil.move(log_file, cog_dir + 'log_file.txt')
#         s3_upload_cogs(glob.glob(cog_dir + '*log_file.txt'), s3_bucket, s3_dir)
                
#         # DELETE ANYTHING WITIN TEH TEMP DIRECTORY
#         cmd = 'rm -frv {}'.format(inter_dir)
#         p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
#         out = p.stdout.read()
        
#         if os.path.exists(inter_dir):
#             print(out)
                
        print('not boo')

            
    except:
        print('boo')
#         root.exception("Processing INCOMPLETE so tidying up")
#         root.removeHandler(handler)
#         handler.close()

#         shutil.move(log_file, cog_dir + 'log_file.txt')
        
#         s3_upload_cogs(glob.glob(cog_dir + '*log_file.txt'), s3_bucket, s3_dir)        
                
#         cmd = 'rm -frv {}'.format(inter_dir)
#         p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
#         out = p.stdout.read()