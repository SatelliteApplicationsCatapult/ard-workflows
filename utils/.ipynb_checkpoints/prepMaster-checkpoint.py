from .prepS2 import *
from utils.yamlUtils import *

import os
from subprocess import Popen, PIPE, STDOUT


def prepare_S2(in_scene_list, output_dir):
    
    """
    Prepare S2 ARD
    - Assumes output_dir already exists...
    """

    non_cogs_dir = output_dir + 'temp/'
    cogs_dir = output_dir
    
    log_file = output_dir + 'log_file.csv'
    
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
            p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
            out = p.stdout.read()

            log.write("{},{},{}".format(des_scene, 'Finish', str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))))
            log.write("\n")
            
    cmd = 'rm -frv {}'.format(cogs_dir + 'temp/')
    p   = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    out = p.stdout.read()