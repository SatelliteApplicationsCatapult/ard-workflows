import tarfile

import requests

from utils.prep_utils import *


def download_extract_ls_url(ls_url, down_tar, untar_dir):
    """
    Download a landsat from ESPA url. 
    Assumes esa hub creds stored as env variables.
    
    :param scene_uuid: S2 download uuid from sentinelsat query
    :param down_dir: directory in which to create a downloaded product dir
    :param original_scene_dir: 
    :return: 
    """
    if not os.listdir(untar_dir):

        if not os.path.exists(down_tar):
            logging.info(f"Downloading tar.gz: {down_tar}")
            resp = requests.get(ls_url)
            open(down_tar, 'wb').write(resp.content)

        logging.info(f"Extracting tar.gz: {down_tar}")
        tar = tarfile.open(down_tar, "r:gz")
        tar.extractall(path=untar_dir)
        tar.close()

    else:
        logging.info(f"Scene already downloaded and extracted: {untar_dir}")


def band_name_l8(prod_path):
    """
    Determine l8 band of individual product from product name
    from path to specific product file
    """

    prod_name = os.path.basename(prod_path)
    prod_name = f"{prod_name.split('_')[-2]}_{prod_name.split('_')[-1][:-4]}"

    logging.debug("Product name is: {}".format(prod_name))

    prod_map = {
        "bt_band10": 'brightness_temperature_1',
        "bt_band11": 'brightness_temperature_2',
        "pixel_qa": 'pixel_qa',
        "radsat_qa": 'radsat_qa',
        "sr_aerosol": 'sr_aerosol',
        "sr_aerosol": 'sr_aerosol',
        "sr_band1": 'coastal_aerosol',
        "sr_band2": 'blue',
        "sr_band3": 'green',
        "sr_band4": 'red',
        "sr_band5": 'nir',
        "sr_band6": 'swir1',
        "sr_band7": 'swir2'
    }

    layer_name = prod_map[prod_name]

    return layer_name


def conv_lsscene_cogs(untar_dir, cog_dir, overwrite=False):
    """
    Convert products to cogs [+ validate TBC].
    
    :param untar_dir: Downloaded S2 product directory (i.e. via ESA or GCloud; assumes .SAFE structure)
    :param cog_dir: directory in which to create the output COGs
    :param overwrite: Binary for whether to overwrite or skip existing COG files)
    :return: 
    """

    if not os.path.exists(untar_dir):
        logging.warning('Cannot find original scene directory: {}'.format(untar_dir))

    # create cog scene directory
    if not os.path.exists(cog_dir):
        logging.info('Creating scene cog directory: {}'.format(cog_dir))
        os.mkdir(cog_dir)
    prod_paths = glob.glob(f"{untar_dir}/*.tif")

    # iterate over prods to create parellel processing list
    for prod in prod_paths:

        in_filename = prod
        out_filename = f"{cog_dir}{os.path.basename(in_filename)[:-4]}.tif"

        # ensure input file exists
        if os.path.exists(in_filename):

            # ensure output cog doesn't already exist
            if not os.path.exists(out_filename):
                conv_sgl_cog(in_filename, out_filename)

            else:
                logging.info('cog already exists: {}'.format(out_filename))
        else:
            logging.warning('cannot find product: {}'.format(in_filename))


def conv_sgl_cog(in_path, out_path):
    """
    Convert a single input file to COG format. Default settings via cogeo repository (funcs within prep_utils). 
    COG val TBC
    
    :param in_path: path to non-cog file
    :param out_path: path to new cog file
    :return: 
    """
    logging.debug(in_path, out_path)
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
        logging.info('not updated nodata')

    # should inc. cog val...


def copy_l8_metadata(untar_dir, cog_dir):
    """
    Parse through LS metadata files.
    
    :param original_scene_dir: downloaded S2 dir in which to find original metadata (MTD_*.xml) file.
    :param cog_scene_dir: dir in which to copy MTD into
    :param scene_name: shortened S2 scene name (i.e. S2A_MSIL2A_20190124T221941_T60KYF from S2A_MSIL2A_20190124T221941_N0211_R029_T60KYF_20190124T234344)
    :return: 
    """
    metas = [fn for fn in glob.glob(f"{untar_dir}*") if
             (".tif" not in os.path.basename(fn)) & ("." in os.path.basename(fn))]
    logging.debug(metas)

    if metas:
        for meta in metas:

            n_meta = f"{cog_dir}{os.path.basename(meta)}"
            logging.info('native_meta: {}'.format(n_meta))

            # check meta file exists
            if os.path.exists(meta):
                # check cp doesn't exist
                if not os.path.exists(n_meta):
                    logging.info("Copying original metadata file to cog dir: {}".format(n_meta))
                    shutil.copyfile(meta, n_meta)
                else:
                    logging.info("Original metadata file already copied to cog_dir: {}".format(n_meta))
            else:
                logging.warning("Cannot find orignial metadata file: {}".format(meta))
    else:
        logging.warning(" No metadata to copy")


def find_l8_datetime(scene_dir):
    try:
        meta = glob.glob(f"{scene_dir}*.xml")[0]
        m = ET.parse(meta).getroot().findall('{http://espa.cr.usgs.gov/v2}global_metadata')[0]  #####
        d = m.find('{http://espa.cr.usgs.gov/v2}acquisition_date').text
        t = m.find('{http://espa.cr.usgs.gov/v2}scene_center_time').text
        return str(datetime.strptime(f"{d}{t[:8]}", '%Y-%m-%d%H:%M:%S'))
    except Exception:
        return str(datetime.strptime(f"{scene_dir.split('_')[-1][:-1]}", '%Y%m%d'))


def yaml_prep_l8(scene_dir):
    """
    Prepare individual L8 scene directory containing L8 cog products converted
    from ESPA-ordered L1T scenes.
    """
    # scene_name = scene_dir.split('/')[-2][:26]
    scene_name = scene_dir.split('/')[-2]
    logging.info(f"Preparing scene {scene_name}")
    logging.info(f"Scene path {scene_dir}")

    # find all cog prods
    prod_paths = glob.glob(scene_dir + '*.tif')
    # print ( 'paths: {}'.format(prod_paths) )
    # for i in prod_paths: print ( i )

    # date time assumed eqv for start and stop - this isn't true and could be
    # pulled from .xml file (or scene dir) not done yet for sake of progression
    t0 = parse(find_l8_datetime(scene_dir))
    t1 = t0

    # get polorisation from each image product (S2 band)
    images = {
        band_name_l8(prod_path): {
            'path': str(prod_path.split('/')[-1])
        } for prod_path in prod_paths
    }

    # trusting bands coaligned, use one to generate spatial bounds for all
    projection, extent = get_geometry('/'.join([str(scene_dir), images['blue']['path']]))

    # parse esa l2a prod metadata file for reference
    scene_genesis = glob.glob(scene_dir + '*.xml')[0]
    if os.path.exists(scene_genesis):
        scene_genesis = os.path.basename(scene_genesis)
    else:
        scene_genesis = ' '

    new_id = str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name))

    return {
        'id': new_id,
        'processing_level': "espa_l2a2cog_ard",
        'product_type': "optical_ard",
        'creation_dt': str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')),
        'platform': {
            'code': 'LANDSAT_8'
        },
        'instrument': {
            'name': 'OLI'
        },
        'extent': create_metadata_extent(extent, t0, t1),
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


def prepareLS(in_scene, s3_bucket='cs-odc-data', s3_dir='common_sensing/fiji/default',
              inter_dir='/tmp/data/intermediate/', prodlevel='L2A'):
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
    ls_url = in_scene
    down_basename = ls_url.split('/')[-1]
    scene_name = f"{down_basename[:4]}_L1TP_{down_basename[4:10]}_{down_basename[10:18]}"
    inter_dir = f"{inter_dir}{scene_name}_tmp/"
    os.makedirs(inter_dir, exist_ok=True)
    down_tar = f"{inter_dir}{down_basename}"
    untar_dir = f"{inter_dir}{scene_name}_untar/"
    os.makedirs(untar_dir, exist_ok=True)
    cog_dir = f"{inter_dir}{scene_name}/"
    os.makedirs(cog_dir, exist_ok=True)
    logging.info(f"scene: {scene_name}\nuntar: {untar_dir}\ncog_dir: {cog_dir}")

    root = setup_logging()

    root.info(f"{scene_name} Starting")

    try:

        try:
            root.info(f"{scene_name} DOWNLOADING via ESPA")
            download_extract_ls_url(ls_url, down_tar, untar_dir)
            root.info(f"{scene_name} DOWNLOADed + EXTRACTED")
        except Exception as e:
            root.exception(f"{scene_name} CANNOT BE FOUND")
            raise Exception('Download Error', e)

        try:
            root.info(f"{scene_name} Converting COGs")
            conv_lsscene_cogs(untar_dir, cog_dir)
            root.info(f"{scene_name} COGGED")
        except Exception as e:
            root.exception(f"{scene_name} CANNOT BE COGGED")
            raise Exception('COG Error', e)

        try:
            root.info(f"{scene_name} Copying medata")
            copy_l8_metadata(untar_dir, cog_dir)
            root.info(f"{scene_name} Copied medata")
        except Exception as e:
            root.exception(f"{scene_name} metadata not copied")
            raise Exception('Metadata copy error', e)

        try:
            root.info(f"{scene_name} Creating yaml")
            create_yaml(cog_dir, yaml_prep_l8(cog_dir))
            root.info(f"{scene_name} Created yaml")
        except Exception as e:
            root.exception(f"{scene_name} yam not created")
            raise Exception('Yaml error', e)

        # MOVE COG DIRECTORY TO OUTPUT DIRECTORY
        try:
            root.info(f"{scene_name} Uploading to S3 Bucket")
            s3_upload_cogs(glob.glob(cog_dir + '*'), s3_bucket, s3_dir)
            root.info(f"{scene_name} Uploaded to S3 Bucket")
        except Exception as e:
            root.exception(f"{scene_name} Upload to S3 Failed")
            raise Exception('S3  upload error', e)

        # DELETE ANYTHING WITIN TEH TEMP DIRECTORY
        clean_up(inter_dir)

    except Exception as e:
        logging.error(f"Could not process {scene_name}", e)
        clean_up(inter_dir)
