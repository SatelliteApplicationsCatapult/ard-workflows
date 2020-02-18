import logging
import os
import shutil
from datetime import datetime
from random import randint
from time import sleep
from urllib.request import urlopen, HTTPPasswordMgrWithDefaultRealm, HTTPBasicAuthHandler, HTTPDigestAuthHandler, build_opener
from urllib.error import HTTPError

from asynchronousfilereader import AsynchronousFileReader
import boto3
import click
import gdal
import rasterio
import requests
import platform
import subprocess
import yaml
import base64
from osgeo import osr
from rasterio.enums import Resampling
from rasterio.env import GDALVersion
from rasterio.io import MemoryFile
from rasterio.shutil import copy
import numpy as np


def to_cog(input_file, output_file, nodata=0):
    if os.path.exists(input_file):
        # ensure output cog doesn't already exist
        if not os.path.exists(output_file):
            conv_sgl_cog(input_file, output_file, nodata=nodata)
        else:
            logging.info(f'cog already exists: {output_file}')
    else:
        logging.warning(f'cannot find product: {input_file}')


def conv_sgl_cog(in_path, out_path, nodata=0):
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
        b.SetNoDataValue(nodata)
        b.FlushCache()
        b = None
        ds = None
    else:
        logging.info('not updated nodata')

    # should inc. cog val...


def clean_up(work_dir):
    # TODO: sort out logging changes...
    shutil.rmtree(work_dir)
    pass


def setup_logging():

    logging.basicConfig(level=logging.DEBUG)
    root = logging.getLogger()
    root.setLevel(os.environ.get("LOGLEVEL", "DEBUG"))

    # Turn down rasterio. It is extremely chatty at debug level.
    logging.getLogger("rasterio").setLevel("INFO")
    logging.getLogger("rasterio._io").setLevel("WARNING")

    # Boto Core is also very chatty at debug. Logging entire request text etc
    logging.getLogger("botocore").setLevel("INFO")
    logging.getLogger("boto").setLevel("INFO")
    logging.getLogger("boto3.resources").setLevel("INFO")
    logging.getLogger("s3transfer").setLevel("INFO")
    logging.getLogger("urllib3").setLevel("INFO")

    return root


def run_snap_command(command, timeout =  60*45):
    """
    Run a snap command. Internal use.

    :param command: the list of arguments to pass to snap
    :return: None
    """

    # if we need to prepend the snap executable.
    if command[0] != os.environ['SNAP_GPT']:
        full_command = [os.environ['SNAP_GPT']] + command
    else:
        full_command = command

    # on linux there is a warning message printed by snap if this environment variable is not set.
    base_env = os.environ.copy()
    if "LD_LIBRARY_PATH" not in base_env and platform.system() != "Windows":
        base_env["LD_LIBRARY_PATH"] = "."

    logging.debug(f"running {full_command}")

    process = subprocess.Popen(full_command, env=base_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.timeout = timeout
    snap_logger_out = logging.getLogger("snap_stdout")
    snap_logger_err = logging.getLogger("snap_stderr")
    std_out_reader = AsynchronousFileReader(process.stdout)
    std_err_reader = AsynchronousFileReader(process.stderr)

    def pass_logging():
        while not std_out_reader.queue.empty():
            line = std_out_reader.queue.get().decode()
            snap_logger_out.info(line.rstrip('\n'))
        while not std_err_reader.queue.empty():
            line = std_err_reader.queue.get().decode()
            snap_logger_err.info("stderr:" + line.rstrip('\n'))
    try:
        while process.poll() is None:
            pass_logging()

        std_out_reader.join()
        std_err_reader.join()
    except subprocess.TimeoutExpired as e :
        logging.error(f"IGNORING subprocess timeout running {command}")
        return
    if process.returncode != 0:
        raise Exception("Snap returned non zero exit status")


def get_file(url, output_path, user=None, password=None):
    logging.debug(f"downloading {url} to {output_path}")
    request = get_url(url, user, password)
    if request:
        with open(output_path, 'wb') as f:
            logging.info(f.write(request.content))


def get_url(url, user=None, password=None):
    """
    Fetch a url and return the content as a byte array
    :param url: the url to go and fetch
    :param user: optional http username to apply to the request
    :param password: optional http password to apply to the request
    :return: byte array containing the file.
    """
    retry = 0
    max_retry = int(os.getenv("DOWNLOAD_RETRY", "3"))
    min_delay = int(os.getenv("DOWNLOAD_MIN_WAIT", "60"))
    max_delay = int(os.getenv("DOWNLOAD_MAX_WAIT", "6000"))

    while retry < max_retry:
        retry += 1
        r = requests.get(url, auth=(user, password))
        if not r.ok:
            if r.status_code == 429:
                delay = randint(min_delay, max_delay)
                logging.error(f"Too many requests. {r.status_code} {r.content.decode('utf-8')}")
                logging.info(f"sleeping for {delay} seconds")
                sleep(delay)
                logging.info("trying again...")
            else:
                logging.error(f"could not make request {r.status_code} {r.content.decode('utf-8')}")
                raise HTTPError(f"could not make request {r.status_code}")
        else:
            return r


def split_all(path):
    """
    split_all takes a path and splits it into a list of directories and files.
    :param path: path to be split
    :return: a list of parts.
    """
    all_parts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            all_parts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            all_parts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            all_parts.insert(0, parts[1])
    return all_parts


def get_geometry(path):
    """
    function stolen and unammended
    """
    logging.debug(f"in get geometry {path}")
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
            # GDAL 3 swapped the parameters around here. 
            # https://github.com/OSGeo/gdal/issues/1546
            lon, lat, z = t.TransformPoint(p['x'], p['y'])
            return {'lon': lon, 'lat': lat}

        extent = {key: transform(p) for key, p in corners.items()}

        return projection, extent


def create_metadata_extent(extent, t0, t1):
    return {
            'coord': extent,
            'from_dt': str(t0),
            'to_dt': str(t1),
            'center_dt': str(t0 + (t1 - t0) / 2)
        }


def create_yaml(scene_dir, metadata):
    """
    Create yaml for single scene directory containing cogs.
    """
    if scene_dir[-1] != '/':
        scene_dir = scene_dir + '/'
    yaml_path = str(scene_dir + 'datacube-metadata.yaml')

    # not sure why default_flow_style is now required - strange...
    with open(yaml_path, 'w') as stream:
        yaml.dump(metadata, stream, default_flow_style=False)

    logging.debug('Created yaml: {}'.format(yaml_path))


def s3_create_client(s3_bucket):
    """
    Create and set up a connection to S3
    :param s3_bucket:
    :return: the s3 client object.
    """

    access = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    session = boto3.Session(
        access,
        secret,
    )

    endpoint = os.getenv("AWS_S3_ENDPOINT")

    if endpoint is not None:
        endpoint_url=f"http://{endpoint}"
        logging.debug('Endpoint URL: {}'.format(endpoint_url))

    if endpoint is not None:
        s3 = session.resource('s3', endpoint_url=endpoint_url)
    else:
        s3 = session.resource('s3', region_name='eu-west-2')

    bucket = s3.Bucket(s3_bucket)

    if endpoint is not None:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            endpoint_url=endpoint_url
        )
    else:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access,
            aws_secret_access_key=secret
        )

    return s3_client, bucket


gb = 1024 ** 3


def s3_single_upload(in_path, s3_path, s3_bucket):
    """
    put a file into S3 from the local file system.

    :param in_path: a path to a file on the local file system
    :param s3_path: where in S3 to put the file.
    :return: None
    """
    
    # prep session & creds
    s3_client, bucket = s3_create_client(s3_bucket)

    # Ensure that multipart uploads only happen if the size of a transfer is larger than
    # S3's size limit for non multipart uploads, which is 5 GB. we copy using multipart 
    # at anything over 4gb
    transfer_config = boto3.s3.transfer.TransferConfig(multipart_threshold=2 * gb,
                                                       max_concurrency=10,
                                                       multipart_chunksize=2 * gb,
                                                       use_threads=True)

    logging.info(f"Local source file: {in_path}")
    logging.info(f"S3 target file: {s3_path}")

    logging.info(f"Start: {in_path} {str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))}")

    s3_client.upload_file(in_path, bucket.name, s3_path)

    # transfer = boto3.s3.transfer.S3Transfer(client=s3_client, config=transfer_config)
    # transfer.upload_file(in_path, bucket.name, s3_path)

    logging.info(f"Finish: {in_path} {str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))}")


def s3_upload_cogs(in_paths, s3_bucket, s3_dir):
    # create upload lists for multi-threading
    out_paths = [s3_dir + i.split('/')[-2] + '/' + i.split('/')[-1]
                 for i in in_paths]

    upload_list = [(in_path, out_path, s3_bucket)
                   for in_path, out_path in zip(in_paths, out_paths)]

    for i in upload_list:
        s3_single_upload(i[0], i[1], i[2])


def s3_list_objects(s3_bucket, prefix):
    # prep session & creds
    client, bucket = s3_create_client(s3_bucket)
    response = client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)

    return response


def s3_list_objects_paths(s3_bucket, prefix):
    """List of paths only returned, not full object responses - tested only for S3"""
    client, bucket = s3_create_client(s3_bucket)
    
    return [e['Key'] for p in client.get_paginator("list_objects_v2").paginate(Bucket=s3_bucket, Prefix=prefix) for e in p['Contents']]


def s3_calc_scene_size(scene_name, s3_bucket, prefix):
    """
    Assumes prefix is directory of scenes like scene_name...
    """

    r = s3_list_objects(s3_bucket, f'{prefix}{scene_name}/')

    return r


def s3_download(s3_bucket, s3_obj_path, dest_path):
    """ - tested only for S3"""
    client, bucket = s3_create_client(s3_bucket)
    
    try:
        bucket.download_file(s3_obj_path, dest_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


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
                                    np.all(matrix != nodata, axis=0).astype(
                                        np.uint8
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


def cog_validate_old(ds, check_tiled=True):
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
            prev_ovr_band = ds.GetRasterBand(1).GetOverview(i - 1)
            if (ovr_band.XSize > prev_ovr_band.XSize or
                    ovr_band.YSize > prev_ovr_band.YSize):
                errors += [
                    'Overview of index %d has larger dimension than '
                    'overview of index %d' % (i, i - 1)]

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
                    (i, ifd_offsets[-1], i - 1, ifd_offsets[-2])]

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
    for i in range(len(data_offsets) - 2, 0, -1):
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


def cog_validate(src_path):
    """
    Validate Cloud Optimized Geotiff.
    Parameters
    ----------
    src_path : str or PathLike object
        A dataset path or URL. Will be opened in "r" mode.
    This script is the rasterio equivalent of
    https://svn.osgeo.org/gdal/trunk/gdal/swig/python/samples/validate_cloud_optimized_geotiff.py
    """
    errors = []
    warnings = []
    details = {}

    if not GDALVersion.runtime().at_least("2.2"):
        raise Exception("GDAL 2.2 or above required")

    config = dict(GDAL_DISABLE_READDIR_ON_OPEN="FALSE")
    with rasterio.Env(**config):
        with rasterio.open(src_path) as src:
            if not src.driver == "GTiff":
                raise Exception("The file is not a GeoTIFF")

            filelist = [os.path.basename(f) for f in src.files]
            src_bname = os.path.basename(src_path)
            if len(filelist) > 1 and src_bname + ".ovr" in filelist:
                errors.append(
                    "Overviews found in external .ovr file. They should be internal"
                )

            overviews = src.overviews(1)
            if src.width > 512 or src.height > 512:
                if not src.is_tiled:
                    errors.append(
                        "The file is greater than 512xH or 512xW, but is not tiled"
                    )

                if not overviews:
                    warnings.append(
                        "The file is greater than 512xH or 512xW, it is recommended "
                        "to include internal overviews"
                    )

            ifd_offset = int(src.get_tag_item("IFD_OFFSET", "TIFF", bidx=1))
            ifd_offsets = [ifd_offset]
            if ifd_offset not in (8, 16):
                errors.append(
                    "The offset of the main IFD should be 8 for ClassicTIFF "
                    "or 16 for BigTIFF. It is {} instead".format(ifd_offset)
                )

            details["ifd_offsets"] = {}
            details["ifd_offsets"]["main"] = ifd_offset

            if overviews and overviews != sorted(overviews):
                errors.append("Overviews should be sorted")

            for ix, dec in enumerate(overviews):

                # NOTE: Size check is handled in rasterio `src.overviews` methods
                # https://github.com/mapbox/rasterio/blob/4ebdaa08cdcc65b141ed3fe95cf8bbdd9117bc0b/rasterio/_base.pyx
                # We just need to make sure the decimation level is > 1
                if not dec > 1:
                    errors.append(
                        "Invalid Decimation {} for overview level {}".format(dec, ix)
                    )

                # Check that the IFD of descending overviews are sorted by increasing
                # offsets
                ifd_offset = int(src.get_tag_item("IFD_OFFSET", "TIFF", bidx=1, ovr=ix))
                ifd_offsets.append(ifd_offset)

                details["ifd_offsets"]["overview_{}".format(ix)] = ifd_offset
                if ifd_offsets[-1] < ifd_offsets[-2]:
                    if ix == 0:
                        errors.append(
                            "The offset of the IFD for overview of index {} is {}, "
                            "whereas it should be greater than the one of the main "
                            "image, which is at byte {}".format(
                                ix, ifd_offsets[-1], ifd_offsets[-2]
                            )
                        )
                    else:
                        errors.append(
                            "The offset of the IFD for overview of index {} is {}, "
                            "whereas it should be greater than the one of index {}, "
                            "which is at byte {}".format(
                                ix, ifd_offsets[-1], ix - 1, ifd_offsets[-2]
                            )
                        )

            block_offset = int(src.get_tag_item("BLOCK_OFFSET_0_0", "TIFF", bidx=1))
            if not block_offset:
                errors.append("Missing BLOCK_OFFSET_0_0")

            data_offset = int(block_offset) if block_offset else None
            data_offsets = [data_offset]
            details["data_offsets"] = {}
            details["data_offsets"]["main"] = data_offset

            for ix, dec in enumerate(overviews):
                data_offset = int(
                    src.get_tag_item("BLOCK_OFFSET_0_0", "TIFF", bidx=1, ovr=ix)
                )
                data_offsets.append(data_offset)
                details["data_offsets"]["overview_{}".format(ix)] = data_offset

            if data_offsets[-1] < ifd_offsets[-1]:
                if len(overviews) > 0:
                    errors.append(
                        "The offset of the first block of the smallest overview "
                        "should be after its IFD"
                    )
                else:
                    errors.append(
                        "The offset of the first block of the image should "
                        "be after its IFD"
                    )

            for i in range(len(data_offsets) - 2, 0, -1):
                if data_offsets[i] < data_offsets[i + 1]:
                    errors.append(
                        "The offset of the first block of overview of index {} should "
                        "be after the one of the overview of index {}".format(i - 1, i)
                    )

            if len(data_offsets) >= 2 and data_offsets[0] < data_offsets[1]:
                errors.append(
                    "The offset of the first block of the main resolution image "
                    "should be after the one of the overview of index {}".format(
                        len(overviews) - 1
                    )
                )

        for ix, dec in enumerate(overviews):
            with rasterio.open(src_path, OVERVIEW_LEVEL=ix) as ovr_dst:
                if ovr_dst.width >= 512 or ovr_dst.height >= 512:
                    if not ovr_dst.is_tiled:
                        errors.append("Overview of index {} is not tiled".format(ix))

    if warnings:
        click.secho("The following warnings were found:", fg="yellow", err=True)
        for w in warnings:
            click.echo("- " + w, err=True)
        click.echo(err=True)

    if errors:
        click.secho("The following errors were found:", fg="red", err=True)
        for e in errors:
            click.echo("- " + e, err=True)

        return False

    return True
