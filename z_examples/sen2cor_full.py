import csv
import os, fnmatch
import urllib.request
import zipfile
import geopandas as gpd
import pandas as pd
import gzip
import glob
from multiprocessing.pool import ThreadPool as Pool
from functools import partial


# [helper to skip directory creation when re-running]
def create_dir(path):
    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)
    else:
        print ("Successfully created the directory %s " % path)


# create required directories based upon main working directory - 50% worth re-creating in fme
# - could probably be completely re-createc?
# - if re-created would require ensuring consistent file locations in other functions
def setup_dirs(in_wd):
    # [create folder structure within working directory]
    create_dir(in_dir + "L1C_gcloud/")
    create_dir(in_dir + "L2A_sen2cor/")
    create_dir(in_dir + "tmp/")
    create_dir(in_dir + "ancillary/")


# download s2 world granule shape file into ancillary directory - 0% worth re-creating in fme
# - needs to be incorporated
def anc_s2_world_grans(in_wd):
    anc_dir = in_wd + "ancillary/"
    s2_grans_zip = anc_dir + "sentinel2_tiles_world.zip"
    s2_grans_shp = anc_dir + "sentinel2_tiles_world/sentinel2_tiles_world.shp"
    if not os.path.exists(s2_grans_shp):
        print('Downloading Sentinel-2 World Granules Shapefile')
        s2_gran_url = "https://docs.google.com/uc?id=0BysUrKXWIDwBZHF6dENlZ0g1Y0k"
        urllib.request.urlretrieve(s2_gran_url, s2_grans_zip)
        with zipfile.ZipFile(s2_grans_zip, "r") as zip_ref:
            zip_ref.extractall(anc_dir)
        print('Downloaded Sentinel-2 World Granules Shapefile')
    return s2_grans_shp


# download google cloud .csv into ancillary directory - 0% worth re-creating in fme
def anc_gcloud_latest_csv(in_wd):
    anc_dir = in_wd + "ancillary/"
    gcloud_s2_csv = anc_dir + "index.csv.gz"
    if not os.path.exists(gcloud_s2_csv):
        print('Downloading latest Sentinel-2 acquisitions and download link from Google')
        csv_url = "https://storage.googleapis.com/gcp-public-data-sentinel-2/index.csv.gz"
        urllib.request.urlretrieve(csv_url, gcloud_s2_csv)
    print('Downloaded latest Sentinel-2 acquisitions and download link from Google')
    return gcloud_s2_csv


# identify desired granules by subsetting world granules shapefile and reading attribute - 90% worth re-creating in fme
def extract_des_granules(in_wd, aoi, s2_world_grans_shp):
    anc_dir = in_wd + "ancillary/"
    # [subset world granules to aoi]
    des_grans_shp = anc_dir + "des_sentinel2_tiles_world.shp"
    if not os.path.exists(des_grans_shp):
        print('Determining Sentinel-2 granules overlapping with Area of Interest')
        cmd = "ogr2ogr -clipsrc %s %s %s" % (aoi, des_grans_shp, s2_world_grans_shp)
        print(cmd)
        os.system(cmd)
    # [extract granule ids from subset file]
    gran_nms = list(gpd.read_file(des_grans_shp)["Name"].values)
    print('Nailed the Sentinel-2 granules overlapping with Area of Interest:')
    print(gran_nms)
    return gran_nms


# query main gcloud .csv and subset to identiied granules, writin to new .csv - 60% worth re-creating in fme
def query_gcloud_csv_aoi(in_wd, in_grans, in_csv_zip):
    anc_dir = in_wd + "ancillary/"
    # query .csv for download locations
    des_acqs = anc_dir + 'aoi_query_df.csv'
    if not os.path.exists(des_acqs):
        print('Filtering every darn Sentinel-2 acquisition for desired granules...')
        with gzip.open(in_csv_zip) as f:
            df = pd.read_csv(f)
            df = df.loc[(df['MGRS_TILE'].isin(in_grans))]
            df.to_csv(des_acqs, sep=',')
            print('Filtered every darn Sentinel-2 acquisition for desired granules...')
            print('Total No. acquisitions across AoI: ', df.shape)
            del df
    return des_acqs


# query aoi .csv and filter by cloud cover and temporal reqs - 60% worth re-creating in fme
def query_aoi_csv(in_csv, cloud_cover, t0, t1):
    print('Filtering by cloud cover and time period.')
    df = pd.read_csv(in_csv)
    df = df.loc[df['CLOUD_COVER'] <= cloud_cover]
    df = df.loc[df['SENSING_TIME'] >= t0]
    df = df.loc[df['SENSING_TIME'] >= t1]
    print('Filtered by cloud cover and time period.')
    print('Number of desired acqs', df.shape)
    return df


# download products from gcloud based upon links within fme - 60% worth re-creating in fme
def download_s2_frm_grans(in_df, out_dir):
    #l1c_dir = in_wd + "L1C_gcloud"
    # download data
    print ( out_dir )
    for i in in_df['BASE_URL'].values:
        print(i)
        cmd = "gsutil -m cp -r %s %s" % (i, out_dir)
        os.system(cmd)


# amend folder structure to be compatible with sen2cor - 0% worth re-creating in fme
def amend_prd_dir_format(in_wd):
    # amend directory format
    # - create AUX & HTML folders
    l1c_dir = in_wd + "L1C_gcloud/"
    print(l1c_dir)
    prod_dir = glob.glob(l1c_dir + "*L1C*.SAFE")
    for prod in prod_dir:
        print(prod)
        create_dir(prod + "/AUX_DATA/")
        create_dir(prod + "/HTML/")


# process a sinle s2 product - 50% re-creating in fme
def process_single(sen2cor, scene):
    cmd = sen2cor + ' ' + scene
    os.system(cmd)
    os.system(cmd)
    scene_2a = scene.replace('_MSIL1C', '_MSIL2A')
    scene_2a_mv = scene_2a.replace('L1C_gcloud', 'L2A_sen2cor')
    os.system("mv %s %s" % (scene_2a, scene_2a_mv))


# multi-thread processing of list of products - 80% worth re-creatin in fme
def process_multi_thread(in_wd, sen2cor):
    l1c_dir = in_wd + "L1C_gcloud/"
    scenes = glob.glob(l1c_dir + '*.SAFE')
    print('Scenes to process: ', scenes)

    func = partial(process_single, sen2cor)

    with Pool(2) as p:
        p.map(func, scenes)


if __name__ == '__main__':

    # eventual arg inputs
    # - location of s2 run file
    in_sen2cor = "/home/tjones/Sen2Cor-02.05.05-Linux64/bin/L2A_Process"
    # - master working directory
    in_dir = "/home/tjones/S2/Auto2/"
    # - area of interest shapefile (wgs84 crs)
    in_aoi = "/home/tjones/S2/test_aoi_uk_4326.geojson"
    # - acquisition window
    in_date_0 = '2018-06-01'
    in_date_1 = '2018-07-01'
    # - cloud cover percentage
    ccp = 1

    # [test input directory exists]
    if not os.path.isdir(in_dir):
        print("working directory %s does not exist" % in_dir)

    setup_dirs(in_dir)
    wrld_grans = anc_s2_world_grans(in_dir)
    gc_csv = anc_gcloud_latest_csv(in_dir)
    des_grans = extract_des_granules(in_dir, in_aoi, wrld_grans)
    aoi_csv = query_gcloud_csv_aoi(in_dir, des_grans, gc_csv)
    df = query_aoi_csv(aoi_csv, ccp, in_date_0, in_date_1)
    download_s2_frm_grans(df, in_dir)
    amend_prd_dir_format(in_dir)
    process_multi_thread(in_dir, in_sen2cor)
