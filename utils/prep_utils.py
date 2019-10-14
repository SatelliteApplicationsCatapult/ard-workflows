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
    if 'LayoverShadow_MASK' in str (prod_name):
        layername = 'layovershadow_mask'
        
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
        
#     t0=parse(str( datetime.strptime(prod_paths[0].split("_")[-5], '%Y%m%dT%H%M%S')))
    t0=parse(str( datetime.strptime(os.path.dirname(prod_paths[0]).split("_")[-1], '%Y%m%dT%H%M%S')))
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
    try:
        projection, extent = get_geometry('/'.join([str(scene_dir), images['vh']['path']]))
    except:
        projection, extent = get_geometry('/'.join([str(scene_dir), images['vv']['path']]))
        print('no vh band available')
    
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


def s3_list_objects(s3_bucket, prefix):
    
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
    
    response = client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    
    return response
    
    
def s3_calc_scene_size(scene_name, s3_bucket, prefix):
    
    """
    Assumes prefix is directory of scenes like scene_name...
    """
    
    r = s3_list_objects(s3_bucket, f'{prefix}{scene_name}/')
    
    return r



"""rio_cogeo.cogeo: translate a file to a cloud optimized geotiff."""
def cog_translate(
    src_path,
    dst_path,
    dst_kwargs,
    indexes=None,
    nodata=None,
    alpha=None,
    overview_level=5,
    overview_resampling=None,
    config=None,
):
    """
    Create Cloud Optimized Geotiff.
    Parameters
    ----------
    src_path : str or PathLike object
        A dataset path or URL. Will be opened in "r" mode.
    dst_path : str or Path-like object
        An output dataset path or or PathLike object.
        Will be opened in "w" mode.
    dst_kwargs: dict
        output dataset creation options.
    indexes : tuple, int, optional
        Raster band indexes to copy.
    nodata, int, optional
        nodata value for mask creation.
    alpha, int, optional
        alpha band index for mask creation.
    overview_level : int, optional (default: 6)
        COGEO overview (decimation) level
    config : dict
        Rasterio Env options.
    """
    config = config or {}

    with rasterio.Env(**config):
        with rasterio.open(src_path) as src:

            indexes = indexes if indexes else src.indexes
            meta = src.meta
            meta["count"] = len(indexes)
            meta.pop("nodata", None)
            meta.pop("alpha", None)

            meta.update(**dst_kwargs)
            meta.pop("compress", None)
            meta.pop("photometric", None)

            with MemoryFile() as memfile:
                with memfile.open(**meta) as mem:
                    wind = list(mem.block_windows(1))
                    for ij, w in wind:
                        matrix = src.read(window=w, indexes=indexes)
                        mem.write(matrix, window=w)

                        if nodata is not None:
                            mask_value = (
                                numpy.all(matrix != nodata, axis=0).astype(
                                    numpy.uint8
                                )
                                * 255
                            )
                        elif alpha is not None:
                            mask_value = src.read(alpha, window=w)
                        else:
                            mask_value = None
                        if mask_value is not None:
                            mem.write_mask(mask_value, window=w)

                    if overview_resampling is not None:
                        overviews = [2 ** j for j in range(1, overview_level + 1)]

                        mem.build_overviews(overviews, Resampling[overview_resampling])
                        mem.update_tags(
                            OVR_RESAMPLING_ALG=Resampling[overview_resampling].name.upper()
                        )

                    copy(mem, dst_path, copy_src_overviews=True, **dst_kwargs)


                    
def cog_validate(ds, check_tiled=True):
    """Check if a file is a (Geo)TIFF with cloud optimized compatible structure.

    Args:
      ds: GDAL Dataset for the file to inspect.
      check_tiled: Set to False to ignore missing tiling.

    Returns:
      A tuple, whose first element is an array of error messages
      (empty if there is no error), and the second element, a dictionary
      with the structure of the GeoTIFF file.

    Raises:
      ValidateCloudOptimizedGeoTIFFException: Unable to open the file or the
        file is not a Tiff.
    """

    if int(gdal.VersionInfo('VERSION_NUM')) < 2020000:
        raise ValidateCloudOptimizedGeoTIFFException(
            'GDAL 2.2 or above required')

    unicode_type = type(''.encode('utf-8').decode('utf-8'))
    if isinstance(ds, str) or isinstance(ds, unicode_type):
        gdal.PushErrorHandler()
        ds = gdal.Open(ds)
        gdal.PopErrorHandler()
        if ds is None:
            raise ValidateCloudOptimizedGeoTIFFException(
                'Invalid file : %s' % gdal.GetLastErrorMsg())
        if ds.GetDriver().ShortName != 'GTiff':
            raise ValidateCloudOptimizedGeoTIFFException(
                'The file is not a GeoTIFF')

    details = {}
    errors = []
    filename = ds.GetDescription()
    main_band = ds.GetRasterBand(1)
    ovr_count = main_band.GetOverviewCount()
    filelist = ds.GetFileList()
    if filelist is not None and filename + '.ovr' in filelist:
        errors += [
            'Overviews found in external .ovr file. They should be internal']

    if main_band.XSize >= 512 or main_band.YSize >= 512:
        if check_tiled:
            block_size = main_band.GetBlockSize()
            if block_size[0] == main_band.XSize and block_size[0] > 1024:
                errors += [
                    'The file is greater than 512xH or Wx512, but is not tiled']

        if ovr_count == 0:
            errors += [
                'The file is greater than 512xH or Wx512, but has no overviews']

    ifd_offset = int(main_band.GetMetadataItem('IFD_OFFSET', 'TIFF'))
    ifd_offsets = [ifd_offset]
    if ifd_offset not in (8, 16):
        errors += [
            'The offset of the main IFD should be 8 for ClassicTIFF '
            'or 16 for BigTIFF. It is %d instead' % ifd_offsets[0]]
    details['ifd_offsets'] = {}
    details['ifd_offsets']['main'] = ifd_offset

    for i in range(ovr_count):
        # Check that overviews are by descending sizes
        ovr_band = ds.GetRasterBand(1).GetOverview(i)
        if i == 0:
            if (ovr_band.XSize > main_band.XSize or
                ovr_band.YSize > main_band.YSize):
                    errors += [
                        'First overview has larger dimension than main band']
        else:
            prev_ovr_band = ds.GetRasterBand(1).GetOverview(i-1)
            if (ovr_band.XSize > prev_ovr_band.XSize or
                ovr_band.YSize > prev_ovr_band.YSize):
                    errors += [
                        'Overview of index %d has larger dimension than '
                        'overview of index %d' % (i, i-1)]

        if check_tiled:
            block_size = ovr_band.GetBlockSize()
            if block_size[0] == ovr_band.XSize and block_size[0] > 1024:
                errors += [
                    'Overview of index %d is not tiled' % i]

        # Check that the IFD of descending overviews are sorted by increasing
        # offsets
        ifd_offset = int(ovr_band.GetMetadataItem('IFD_OFFSET', 'TIFF'))
        ifd_offsets.append(ifd_offset)
        details['ifd_offsets']['overview_%d' % i] = ifd_offset
        if ifd_offsets[-1] < ifd_offsets[-2]:
            if i == 0:
                errors += [
                    'The offset of the IFD for overview of index %d is %d, '
                    'whereas it should be greater than the one of the main '
                    'image, which is at byte %d' %
                    (i, ifd_offsets[-1], ifd_offsets[-2])]
            else:
                errors += [
                    'The offset of the IFD for overview of index %d is %d, '
                    'whereas it should be greater than the one of index %d, '
                    'which is at byte %d' %
                    (i, ifd_offsets[-1], i-1, ifd_offsets[-2])]

    # Check that the imagery starts by the smallest overview and ends with
    # the main resolution dataset
    block_offset = main_band.GetMetadataItem('BLOCK_OFFSET_0_0', 'TIFF')
    if not block_offset:
        errors += ['Missing BLOCK_OFFSET_0_0']
    data_offset = int(block_offset) if block_offset else None
    data_offsets = [data_offset]
    details['data_offsets'] = {}
    details['data_offsets']['main'] = data_offset
    for i in range(ovr_count):
        ovr_band = ds.GetRasterBand(1).GetOverview(i)
        data_offset = int(ovr_band.GetMetadataItem('BLOCK_OFFSET_0_0', 'TIFF'))
        data_offsets.append(data_offset)
        details['data_offsets']['overview_%d' % i] = data_offset

    if data_offsets[-1] < ifd_offsets[-1]:
        if ovr_count > 0:
            errors += [
                'The offset of the first block of the smallest overview '
                'should be after its IFD']
        else:
            errors += [
                'The offset of the first block of the image should '
                'be after its IFD']
    for i in range(len(data_offsets)-2, 0, -1):
        if data_offsets[i] < data_offsets[i + 1]:
            errors += [
                'The offset of the first block of overview of index %d should '
                'be after the one of the overview of index %d' %
                (i - 1, i)]
    if len(data_offsets) >= 2 and data_offsets[0] < data_offsets[1]:
        errors += [
            'The offset of the first block of the main resolution image'
            'should be after the one of the overview of index %d' %
            (ovr_count - 1)]

    return errors, details
