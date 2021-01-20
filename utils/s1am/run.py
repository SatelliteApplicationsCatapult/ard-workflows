import os
import argparse
from raw2ard import Raw2Ard

# parse command line arguments
def parseArguments(args=None):

    """
    Placeholder
    """

    # parse configuration
    parser = argparse.ArgumentParser(description='s1 raw to ard processor')
    parser.add_argument('scene', action="store")

    # optional arguments
    parser.add_argument('--out_path',
                        help='output directory',
                        default=None )

    parser.add_argument('--remove_border_noise',
                        help='enable border noise removal',
                        default=True )

    parser.add_argument('--remove_thermal_noise',
                        help='enable thermal noise removal',
                        default=True )

    parser.add_argument('--terrain_flattening',
                        help='enable terrain flattening',
                        default=True )

    parser.add_argument('--geocoding',
                        help='geocoding type',
                        default='Range-Doppler' )

    parser.add_argument('--target_resolution',
                        help='ard image resolution',
                        default=20.0 )

    parser.add_argument('--external_dem',
                        help='pathname to dem',
                        default=None )

    parser.add_argument('--scaling',
                        help='linear or log units',
                        default='db' )

    return parser.parse_args(args)


# entry point
def main():

    """
    Placeholder
    """

    # parse arguments
    args = parseArguments()
    obj = Raw2Ard( chunks=6, gpt='/opt/snap/bin/gpt' )

    # default output path if not defined
    out_path = args.out_path
    if args.out_path is None:
        out_path = os.path.dirname( args.scene ).replace( 'raw', 'ard' )    

    # execute processing
    obj.process( args.scene, out_path ) #, args )
    return

# execute main
if __name__ == '__main__':
    main()


