try:
    from .prepS2 import *
except:
    from prepS2 import *
try:
    from .yamlUtils import *
except:
    from yamlUtils import *

import os
from subprocess import Popen, PIPE, STDOUT

import click


def prepareS2(scenelist, outputdir, prodlevel='L1C', source='GCloud', tidyup=True):
    """
    Prepare Sentinel-2 datasets for DC ingestion.
    :param scenelist:  List of S2 scene names to be processed (inc. .SAFE extension for now)
    :param outputdir:  Directory into which downloaded and processed sub-dirs will be created
    :param prodlevel:  Desired Sentinel-2 product level. Defaults to 'L1C'. Use 'L2A' for ARD equivalent
    :param source:     Api source to be used for downloading scenes. Defaults to 'GCloud' for L1C products. (If L2A prodlevel specified then defaults to esa.)
    :return: nothing but data...
    """


    
    with open(log_file, 'a') as log:
        
        for des_scene in in_scene_list:
            log.write("{},{},{}".format(des_scene, 'Start', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")
            
            # shorten scene name
            scene_name = des_scene[:-21]
            scene_name = scene_name[:-17] + scene_name.split('_')[-1]
            print ( 'Scene name: {}'.format(scene_name) )

            down_dir = non_cogs_dir + des_scene + '/'
            download_s2_granule_gcloud(des_scene, non_cogs_dir)

            log.write("{},{},{}".format(des_scene, 'Downloaded', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")

            # convert to cog
            cog_dir = cogs_dir + scene_name + '/'
            print ( 'COG dir: {}'.format(cog_dir) )
            conv_s2scene_cogs(down_dir, cog_dir, scene_name)

            log.write("{},{},{}".format(des_scene, 'COG', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")

            # Keep original metadata
            copy_s2_metadata(down_dir, cog_dir, scene_name)

            # Generate yaml
            create_yaml(cog_dir, 's2')

            log.write("{},{},{}".format(des_scene, 'Yaml', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")
            
            cmd = 'rm -frv {}'.format(down_dir)
            try:
                p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
                out = p.stdout.read()
            except:
                print('error removing processing directory, will try again once queue complete: {}'.format(out))
            
            log.write("{},{},{}".format(des_scene, 'Finish', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")
            
    cmd = 'rm -frv {}'.format(cogs_dir + 'temp/')
    p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    out = p.stdout.read()
    
    
@click.command()
@click.option('--scene_name', help='Input scene name')
@click.option('--out_dir', default=False, help='Output directory')
def main(scene_name, out_dir):

    print('Processing {} into {}'.format(scene_name, out_dir))

        non_cogs_dir = output_dir + 'temp/'
    cogs_dir = output_dir
    
    log_file = output_dir + 'log_file.csv'
        
        
if __name__ == '__main__':

    main()

