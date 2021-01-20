import math
import zipfile
from subprocess import Popen, PIPE, STDOUT
from dateutil.parser import parse
import glob
import numpy
import pandas as pd
from requests import HTTPError
from http.cookiejar import MozillaCookieJar
from urllib.request import Request, HTTPHandler, HTTPSHandler, HTTPCookieProcessor, build_opener
from urllib.error import HTTPError
import uuid
from sentinelsat import SentinelAPI

from utils.prep_utils import *

from utils.s1am.raw2ard import Raw2Ard

cookie_jar_path = os.path.join( os.path.expanduser('~'), ".bulk_download_cookiejar.txt")
cookie_jar = MozillaCookieJar()


def get_asf_cookie(user, password):

    logging.info("logging into asf")

    login_url = "https://urs.earthdata.nasa.gov/oauth/authorize"
    client_id = "BO_n7nTIlMljdvU6kRRB3g"
    redirect_url = "https://auth.asf.alaska.edu/login"

    user_pass = base64.b64encode(bytes(user + ":" + password, "utf-8"))
    user_pass = user_pass.decode("utf-8")

    auth_cookie_url = f"{login_url}?client_id={client_id}&redirect_uri={redirect_url}&response_type=code&state="
    context = {}
    opener = build_opener(HTTPCookieProcessor(cookie_jar), HTTPHandler(), HTTPSHandler(**context))
    request = Request(auth_cookie_url, headers={"Authorization": "Basic {0}".format(user_pass)})

    try:
        response = opener.open(request)
    except HTTPError as e:
        if e.code == 401:
            logging.error("invalid username and password")
            return False
        else:
            # If an error happens here, the user most likely has not confirmed EULA.
            logging.error(f"Could not log in. {e.code} {e.response}")
            return False

    if check_cookie_is_logged_in(cookie_jar):
        # COOKIE SUCCESS!
        cookie_jar.save(cookie_jar_path)
        logging.info("successfully logged into asf")
        return True

    logging.info("failed logging into asf")
    return False


def check_cookie_is_logged_in(cj):
    for cookie in cj:
        if cookie.name == 'urs_user_already_logged':
            # Only get this cookie if we logged in successfully!
            return True

    return False


def get_s1_asf_urls(s1_name_list):
    df = pd.DataFrame()

    num_parts = math.ceil(len(s1_name_list) / 119)
    s1_name_lists = numpy.array_split(numpy.array(s1_name_list), num_parts)
    logging.debug([len(l) for l in s1_name_lists])

    for l in s1_name_lists:
        try:
            df = df.append(
                pd.read_csv(
                    f"https://api.daac.asf.alaska.edu/services/search/param?granule_list={','.join(l)}&output=csv"
                ),
                ignore_index=True
            )
        except Exception as e:
            logging.error(e)

    return df.loc[df['Processing Level'] == 'GRD_HD']


def get_s1_asf_url(s1_name, retry=3):
    """
    Finds Alaska Satellite Facility download url for single S1_NAME Sentinel-1 scene. 

    :param s1_name: Scene ID for Sentinel Tile (i.e. "S1A_IW_SLC__1SDV_20190411T063207_20190411T063242_026738_0300B4_6882")
    :param retry: number of times to retry
    :return s1url:download url
    :return False: unable to find url
    """
    logging.info(f"fetching: https://api.daac.asf.alaska.edu/services/search/param?granule_list={s1_name}&output=csv")
    try:
        return pd \
            .read_csv(f"https://api.daac.asf.alaska.edu/services/search/param?granule_list={s1_name}&output=csv") \
            .URL \
            .values[0]
    except HTTPError as e:
        logging.debug(f"could not query: {e}", )
        if e.code == 503 and retry > 0:
            logging.info("retrying...")
            return get_s1_asf_url(s1_name, retry - 1)
        return 'NaN'
    except Exception as e:
        logging.debug(f"could not query: {e}")
        return 'NaN'


def get_asf_file(url, output_path, chunk_size=16*1024):  # 8 kb default
    context = {}
    opener = build_opener(HTTPCookieProcessor(cookie_jar), HTTPHandler(), HTTPSHandler(**context))
    request = Request(url)
    response = opener.open(request)

    with open(output_path, "wb") as f:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)


def download_extract_s1_scene_asf(s1_name, download_dir):
    """
    Downloads single S1_NAME Sentinel-1 scene into DOWLOAD_DIR. 

    :param s1_name: Scene ID for Sentinel Tile (i.e. "S1A_IW_SLC__1SDV_20190411T063207_20190411T063242_026738_0300B4_6882")
    :param download_dir: path to directory for downloaded S1 granule
    :return:
    """

    if s1_name.endswith('.SAFE'):
        s1_name = s1_name[:-5]

    # Grab url for scene
    s1url = get_s1_asf_url(s1_name)
    if s1url == 'NaN':
        logging.error(f"did not get a valid url. Aborting download of {s1_name}")
        return

    asf_user = os.getenv("ASF_USERNAME")
    asf_pwd = os.getenv("ASF_PWD")

    # Extract downloaded .zip file
    zipped = os.path.join(download_dir, s1_name + '.zip')
    safe_dir = os.path.join(download_dir, s1_name + '.SAFE/')

    if not check_cookie_is_logged_in(cookie_jar):
        get_asf_cookie(asf_user, asf_pwd)

    if not os.path.exists(zipped) & (not os.path.exists(safe_dir)):
        get_asf_file(s1url, zipped)

#     if not os.path.exists(safe_dir):
#         logging.info('Extracting ASF scene: {}'.format(zipped))
#         zip_ref = zipfile.ZipFile(zipped, 'r')
#         zip_ref.extractall(os.path.dirname(download_dir))
#         zip_ref.close()


def find_s1_uuid(s1_filename):
    """
    Returns S2 uuid required for download via sentinelsat, based upon an input S2 file/scene name. 
    I.e. S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410
    Assumes esa hub creds stored as env variables.
    
    :param s2_file_name: Sentinel-2 scene name
    :return s2_uuid: download id
    """
    copernicus_username = os.getenv("COPERNICUS_USERNAME")
    copernicus_pwd = os.getenv("COPERNICUS_PWD")
    logging.debug(f"ESA username: {copernicus_username}")
    esa_api = SentinelAPI(copernicus_username, copernicus_pwd)

    if s1_filename[-5:] == '.SAFE':
        res = esa_api.query(filename=s1_filename)
        res = esa_api.to_geodataframe(res)

        return res.uuid.values[0]
        
def download_extract_s1_esa(scene_uuid, down_dir, original_scene_dir):
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
        if not os.path.exists(original_scene_dir.replace('.SAFE/', '.zip')):
            logging.info('Downloading ESA scene zip: {}'.format(os.path.basename(original_scene_dir)))

            copernicus_username = os.getenv("COPERNICUS_USERNAME")
            copernicus_pwd = os.getenv("COPERNICUS_PWD")
            logging.debug(f"ESA username: {copernicus_username}")
            esa_api = SentinelAPI(copernicus_username, copernicus_pwd)
            esa_api.download(scene_uuid, down_dir, checksum=True)
        # extract downloaded .zip file
#         logging.info('Extracting ESA scene: {}'.format(original_scene_dir))
#         zip_ref = zipfile.ZipFile(original_scene_dir.replace('.SAFE/', '.zip'), 'r')
#         zip_ref.extractall(os.path.dirname(down_dir))
#         zip_ref.close()

    else:
        logging.warning('ESA scene already extracted: {}'.format(original_scene_dir))

    # remove zipped scene but onliy if unzipped 
    if os.path.exists(original_scene_dir) & os.path.exists(original_scene_dir.replace('.SAFE/', '.zip')):
        logging.info('Deleting ESA scene zip: {}'.format(original_scene_dir.replace('.SAFE/', '.zip')))
        os.remove(original_scene_dir.replace('.SAFE/', '.zip'))


def band_name_s1(prod_path):
    """
    Determine polarisation of individual product from product name
    from path to specific product file
    """

    prod_name = str(prod_path.split('/')[-1])

    if 'Gamma0_VH' in str(prod_name):
        return 'vh'
    if 'Gamma0_VV' in str(prod_name):
        return 'vv'
    if 'LayoverShadow_MASK' in str(prod_name):
        return 'layovershadow_mask'

    logging.error(f"could not find band name for {prod_path}")

    return 'unknown layer'


def conv_s1scene_cogs(noncog_scene_dir, cog_scene_dir, scene_name, overwrite=False):
    """
    Convert S1 scene products to cogs [+ validate].
    """

    if not os.path.exists(noncog_scene_dir):
        logging.warning('Cannot find non-cog scene directory: {}'.format(noncog_scene_dir))

    # create cog scene directory - replace with one lined os.makedirs(exists_ok=True)
    if not os.path.exists(cog_scene_dir):
        logging.warning('Creating scene cog directory: {}'.format(cog_scene_dir))
        os.mkdir(cog_scene_dir)

    des_prods = ["Gamma0_VV_db",
                 "Gamma0_VH_db",
                 "LayoverShadow_MASK_VH"]  # to ammend once outputs finalised - TO DO*****

    # find all individual prods to convert to cog (ignore true colour images (TCI))
    prod_paths = glob.glob(noncog_scene_dir + '*TF_TC*/*.tif')  # - TO DO*****
    prod_paths = [x for x in prod_paths if os.path.basename(x)[:-4] in des_prods]
#     print(noncog_scene_dir)
#     print(glob.glob(noncog_scene_dir + '/*TF_TC*/'))
#     print(prod_paths)
    
    # iterate over prods to create parellel processing list
    for prod in prod_paths:
        out_filename = os.path.join(cog_scene_dir, scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
        logging.info(f"converting {prod} to cog at {out_filename}")
        # ensure input file exists
        to_cog(prod, out_filename, nodata=-9999)


def copy_s1_metadata(out_s1_prod, cog_scene_dir, scene_name):
    """
    Parse through S2 metadtaa .xml for either l1c or l2a S2 scenes.
    """

    if os.path.exists(out_s1_prod):

        meta_base = os.path.basename(out_s1_prod)
        n_meta = os.path.join(cog_scene_dir + '/' + scene_name + '_' + meta_base)
        logging.info("Copying original metadata file to cog dir: {}".format(n_meta))
        if not os.path.exists(n_meta):
            shutil.copyfile(out_s1_prod, n_meta)
        else:
            logging.info("Original metadata file already copied to cog_dir: {}".format(n_meta))
    else:
        logging.warning("Cannot find orignial metadata file: {}".format(out_s1_prod))


def yaml_prep_s1(scene_dir):
    """
    Prepare individual S1 scene directory containing S1 products
    note: doesn't inc. additional ancillary products such as incidence
    angle or layover/foreshortening masks
    """
    scene_name = scene_dir.split('/')[-2]
    logging.info("Preparing scene {}".format(scene_name))
    logging.info("Scene path {}".format(scene_dir))

    prod_paths = glob.glob(os.path.join(scene_dir, '*.tif'))

    logging.info(f"prod_path: {prod_paths}, scene_name: {scene_name}")
    t0 = parse(str(datetime.strptime(scene_name.split("_")[-2], '%Y%m%dT%H%M%S')))

    # get polorisation from each image product (S1 band)
    # should be replaced with a more concise, generalisable parsing
    images = {
        band_name_s1(prod_path): {
            'path': os.path.split(prod_path)[-1]
        } for prod_path in prod_paths
    }
    logging.debug(images)

    # trusting bands coaligned, use one to generate spatial bounds for all
    try:
        projection, extent = get_geometry(os.path.join(str(scene_dir), images['vh']['path']))
    except:
        projection, extent = get_geometry(os.path.join(str(scene_dir), images['vv']['path']))
        logging.warning('no vh band available')

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
            'source_datasets': {},
        }

    }


def prepareS1AM(in_scene, chunks=24, ext_dem=True, s3_bucket='public-eo-data', s3_dir='common_sensing/sentinel_1/', inter_dir='/tmp/data/intermediate/',
                source='asf'):
    """
    Prepare IN_SCENE of Sentinel-1 satellite data into OUT_DIR for ODC indexing. 

    :param in_scene: input Sentinel-1 scene name (either L1C or L2A) i.e. "S2A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410.SAFE"
    :param out_dir: output directory to drop COGs into.
    :param --inter: optional intermediary directory to be used for processing.
    :param --source: Api source to be used for downloading scenes. Defaults to gcloud. Options inc. 'gcloud', 'esahub', 'sedas' COMING SOON
    :return: None
    
    Assumptions:
    - etc.... tbd
    """

    # Need to handle inputs with and without .SAFE extension
    if not in_scene.endswith('.SAFE'):
        in_scene = in_scene + '.SAFE'
    # shorten scene name
    scene_name = in_scene[:32]

    # Unique inter_dir needed for clean-up
    inter_dir = inter_dir + scene_name + '_tmp/'
    # sub-dirs used only for accessing tmp files
    cog_dir = os.path.join(inter_dir, scene_name)
    os.makedirs(cog_dir, exist_ok=True)
    # s1-specific relative inputs
#     input_mani = inter_dir + in_scene + '/manifest.safe'
#     inter_prod1 = inter_dir + scene_name + '_Orb_Cal_Deb_ML.dim'
#     inter_prod1_dir = inter_prod1[:-4] + '.data/'
#     inter_prod2 = inter_dir + scene_name + '_Orb_Cal_Deb_ML_TF.dim'
#     inter_prod2_dir = inter_prod2[:-4] + '.data/'
#     out_prod1 = inter_dir + scene_name + '_Orb_Cal_Deb_ML_TF_TC_dB.dim'
#     out_dir1 = out_prod1[:-4] + '.data/'
#     out_prod2 = inter_dir + scene_name + '_Orb_Cal_Deb_ML_TF_TC_lsm.dim'
#     out_dir2 = out_prod2[:-4] + '.data/'
    down_dir = inter_dir + in_scene + '/'
    
#     snap_gpt = os.environ['SNAP_GPT']
#     int_graph_1 = os.environ['S1_PROCESS_P1A']  # ENV VAR
# #     if ext_dem:
# #         ext_dem_path = inter_dir + 'ext_dem.tif'
#     int_graph_2 = os.environ['S1_PROCESS_P2A']  # ENV VAR
#     int_graph_3 = os.environ['S1_PROCESS_P3A']  # ENV VAR
#     int_graph_4 = os.environ['S1_PROCESS_P4A']  # ENV VAR


    down_zip = inter_dir + in_scene.replace('.SAFE','.zip')
    am_dir = down_zip.replace('.zip', 'Orb_Cal_Deb_ML_TF_TC_dB/')
    
    root = setup_logging()
    root.info('{} {} Starting'.format(in_scene, scene_name))

    try:

        #  DOWNLOAD
        try:
            root.info(f"{in_scene} {scene_name} DOWNLOADING via ASF")
            download_extract_s1_scene_asf(in_scene, inter_dir)
#             raise Exception('skipping asf for testing')
            root.info(f"{in_scene} {scene_name} DOWNLOADED via ASF")
        except Exception as e:
            root.exception(e)
            root.exception(f"{in_scene} {scene_name} UNAVAILABLE via ASF, try ESA")
            try:
                s1id = find_s1_uuid(in_scene)
                logging.debug(s1id)
                root.info(f"{in_scene} {scene_name} AVAILABLE via ESA")
                download_extract_s1_esa(s1id, inter_dir, down_dir)
                root.info(f"{in_scene} {scene_name} DOWNLOADED via ESA")
            except Exception as e:
                root.exception(f"{in_scene} {scene_name} UNAVAILABLE via ESA too")
                raise Exception('Download Error ESA', e)

        try:
            root.info(f"{in_scene} {scene_name} DOWNLOADING External DEMs")
            # Download external DEMs
            ext_dem_path_E = "common_sensing/ancillary_products/SRTM1Sec/SRTM30_Fiji_E.tif"
            ext_dem_path_W = "common_sensing/ancillary_products/SRTM1Sec/SRTM30_Fiji_W.tif"
            ext_dem_E = f'{inter_dir}SRTM30_Fiji_E.tif'
            ext_dem_W = f'{inter_dir}SRTM30_Fiji_W.tif'
            if not os.path.exists(ext_dem_path_E):
                s3_download(s3_bucket, ext_dem_path_E, ext_dem_E)
                s3_download(s3_bucket, ext_dem_path_W, ext_dem_W)
            root.info(f"{in_scene} {scene_name} DOWNLOADED E+W DEMs")
        except Exception as e:
            root.exception(e)
            root.exception(f"{ext_dem_path_E} or {ext_dem_path_W} UNAVAILABLE")
        
        try:
            root.info(f"{in_scene} {scene_name} Starting AM SNAP processing")        
            # Do AM Processing
            obj = Raw2Ard( chunks=chunks, gpt='/opt/snap/bin/gpt' )
            obj.process(down_zip, am_dir, ext_dem_E, ext_dem_W)
        except Exception as e:
            root.exception(e)
            root.exception(f"Processing failed")
        

        # CONVERT TO COGS TO TEMP COG DIRECTORY**
        try:
            root.info(f"{in_scene} {scene_name} Converting COGs")
            conv_s1scene_cogs(inter_dir, cog_dir, scene_name)
            root.info(f"{in_scene} {scene_name} COGGED")
        except Exception as e:
            root.exception(f"{in_scene} {scene_name} COG conversion FAILED")
            raise Exception('COG Error', e)

#             # PARSE METADATA TO TEMP COG DIRECTORY**
#         try:
#             root.info(f"{in_scene} {scene_name} Copying original METADATA")
#             copy_s1_metadata(out_prod1, cog_dir, scene_name)
#             copy_s1_metadata(out_prod2, cog_dir, scene_name)
#             root.info(f"{in_scene} {scene_name} COPIED original METADATA")
#         except Exception as e:
#             root.exception(f"{in_scene} {scene_name} MTD not coppied")
#             raise e

        # GENERATE YAML WITHIN TEMP COG DIRECTORY**
        try:
            root.info(f"{in_scene} {scene_name} Creating dataset YAML")
            create_yaml(cog_dir, yaml_prep_s1(cog_dir))
            root.info(f"{in_scene} {scene_name} Created original METADATA")
        except Exception as e:
            root.exception(f"{in_scene} {scene_name} Dataset YAML not created")
            raise Exception('YAML creation error', e)

            # MOVE COG DIRECTORY TO OUTPUT DIRECTORY
        try:
            root.info(f"{in_scene} {scene_name} Uploading to S3 Bucket")
            s3_upload_cogs(glob.glob(os.path.join(cog_dir, '*')), s3_bucket, s3_dir)
            root.info(f"{in_scene} {scene_name} Uploaded to S3 Bucket")
        except Exception as e:
            root.exception(f"{in_scene} {scene_name} Upload to S3 Failed")
            raise Exception('S3  upload error', e)
        print('not boo')

        # DELETE ANYTHING WITHIN THE TEMP DIRECTORY
        clean_up(inter_dir)

    except Exception as e:
        logging.error(f"could not process {scene_name} {e}")
        print('boo')
        clean_up(inter_dir)


if __name__ == '__main__':
    prepareS1AM('S1A_IW_GRDH_1SDV_20191001T064008_20191001T064044_029261_035324_C74C',
              s3_dir='fiji/Sentinel_1_dockertest/', inter_dir='../S1_ARD/')
