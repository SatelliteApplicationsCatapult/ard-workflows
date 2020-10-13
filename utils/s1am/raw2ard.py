import os
import re
import sys
import math
import copy
import shutil
import xmltodict

from . import metadata
from . import utility

from osgeo import gdal
from pathlib import Path
from datetime import datetime
from collections import OrderedDict
from . densifygrid import DensifyGrid

import pdb

class Raw2Ard:

    def __init__( self, chunks=6, gpt='/opt/snap/bin/gpt' ):

        """
        constructor function
        """

        # get xml schema
#         with open ( './s1am/recipes/base.xml' ) as fd: # make var path
#             self._base = xmltodict.parse( fd.read() )

#         with open ( os.getenv( 'S1_PROCESS_P1A' ) ) as fd: # make var path
        with open ( './utils/s1am/recipes/cs_base.xml' ) as fd: # make var path
            self._pt1 = xmltodict.parse( fd.read() )

        self._densify = DensifyGrid()
        self._fat_swath = 10.0

        self._gpt = gpt
        self._chunks = chunks

        return


    def process ( self, scene, out_path, E_DEM, W_DEM, args=None  ):

        """
        entry point to class functionality
        """

        # update arguments
        self.getArguments( args )
        tmp_path = os.path.join( out_path, 'tmp' )
        
        if os.path.isdir( tmp_path ):
            shutil.rmtree( tmp_path )

        # extract scene zip 
#         print ( 'Extracting dataset: {}'.format( scene ) )
        dataset_files = utility.unpackFiles( scene, '(.*?)', tmp_path )
#         print ( '... OK!' )

        # load metadata into dictionary
        meta = metadata.getManifest( utility.matchFile( dataset_files, '.*\/manifest.safe' ) )
        meta.update( metadata.getAnnotation( utility.matchFile( dataset_files, '.*\/annotation\/s1.*vv.*\.xml' ) ) )
        
        # overall output product scene name
        product = meta[ 'product' ]
        nm = 'S1{}_{}_{}_{}'.format(  product[ 'satellite' ], 
                                        product[ 'mode' ],
                                        meta[ 'acquisition' ][ 'start' ].strftime( '%y%m%dT%H%M%S' ),
                                        'bnr_orb_cal_ml_tf_tc_db' )
        outname = os.path.join( tmp_path, nm )
#         print( f'OUTNAME: { outname }' )
        
        ##### determine if scene crosses antemeridian #####
        extent = self.getSceneExtent( meta )
        if extent[ 'lon' ][ 'max' ] - extent[ 'lon' ][ 'min' ] > self._fat_swath:

            # densify annotated geolocation grid
            self._densify.process( utility.matchFiles( dataset_files, '.*\/annotation\/s1.*\.xml' ), grid_pts=250 )
            meta.update( metadata.getGeolocationGrid( utility.matchFile( dataset_files, '.*\/annotation\/s1.*vv.*\.xml' ) ) )

            # split gcps into east / west sub-groups
            gcps = self.splitGcps( meta[ 'gcps' ] )
            chunk_size = int ( math.ceil ( float ( meta[ 'image' ][ 'lines' ] ) / float ( self._chunks ) ) )
    
            # process subset blocks either side of antemeridian
            results = []
            for hemisphere in [ 'east', 'west' ]:

                # for each row block
                start_row = 0; offset = 10  # ensure subsets overlap
                while start_row < meta[ 'image' ][ 'lines' ]:

                    # derive subset parameters
                    block = {   'start' : max( start_row - offset, 0 ),
                                'end' : min ( start_row + chunk_size + offset, meta[ 'image' ][ 'lines' ] - 1 ),
                                'samples' : meta[ 'image' ][ 'samples' ],
                                'lines' : meta[ 'image' ][ 'lines' ] }

                    subset = self.getSubset( gcps[ hemisphere ], block )

                    # unq subset name
                    subset_name = '_'.join( str ( int( x ) ) for x in subset )
#                     print ( 'Processing {} subset: {}'.format( hemisphere, subset_name ) )
                    
                    ######################### PT1 - ####################################
                    ##### load PT1 schema #####
            #         schema = self.buildSchema( copy.deepcopy( self._base ), meta )
                    schema = copy.deepcopy( self._pt1 ) 
#                     print( schema )
                    
#                     print('dataset_files: ', dataset_files)
                    
                    ##### set parameters of reader task #####
                    param = self.getParameterSet( schema, 'Read' )
                    param[ 'file' ] = dataset_files[ 0 ] + '/manifest.safe'       # parent path to extracted dataset
                    param[ 'formatName' ] = 'SENTINEL-1'

                    ##### insert subset task #####
                    schema = self.insertNewTask( schema, 'Subset', after='Read' )
                    param = self.getParameterSet ( schema, 'Subset' )
                    param[ 'geoRegion' ] = ''
                    
                    ##### copy subset values into schema dictionary #####
                    param = self.getParameterSet ( schema, 'Subset' )
                    param[ 'region' ] = ','.join( str ( int( x ) ) for x in subset )

                    ##### ext dem input file #####
                    param = self.getParameterSet ( schema, 'Terrain-Flattening' )
                    if hemisphere == 'west':
                        param[ 'externalDEMFile' ] = W_DEM
                    elif hemisphere == 'east':
                        param[ 'externalDEMFile' ] = E_DEM

                    ##### ext dem input file #####
                    param = self.getParameterSet ( schema, 'Terrain-Correction' )            
                    if hemisphere == 'west':
                        param[ 'externalDEMFile' ] = W_DEM
                    elif hemisphere == 'east':
                        param[ 'externalDEMFile' ] = E_DEM
                        
                    # create subset-specific output path
#                     param = self.getParameterSet ( schema, 'Write(3)' )            
                    param = self.getParameterSet ( schema, 'Write' )   
                    param['formatName'] = 'ENVI'
                    
#                     outname_pt1 = os.path.join( outname, 'subset_'+ subset_name + '_bnr_orb_cal_ml')
                    outname_pt1 = os.path.join( outname, 'subset_'+ subset_name + '_Orb_Cal_Deb_ML_TF_TC_dB')
#                     print( 'PT1 OUTNAME: ', outname_pt1 )
                    param[ 'file' ] = outname_pt1
#                     param[ 'file' ] = os.path.join( outname, outname_pt1 + 'subset_' + subset_name )
                    results.append( param['file'] ) # needs to be final output for each subset ############

                    # transform dict back to xml schema & save serialised xml schema to file
                    out = xmltodict.unparse( schema, pretty=True )
                    cfg_pathname = os.path.join ( tmp_path, '{}.xml'.format( os.path.basename(outname_pt1) ) )
                    with open( cfg_pathname, 'w+') as file:
                        file.write(out)

                    ##### execute PT1 processing for subset #####
#                     print ( 'Processing PT1 {} subset: {}'.format( hemisphere, subset_name ) )
                    out, err, code = utility.execute( self._gpt, [ cfg_pathname ] )
#                     print ( f'OUT: { out }' )
#                     print ( f'ERR: { err }' )
#                     print ( '... OK!' )

            
                    # move onto next block
                    start_row += chunk_size
            
#             print('mosaic VV')
            # mosaic subsets into single image
            self.generateImage( out_path, results, 'VV' )
#             print('mosaic VH')
            self.generateImage( out_path, results, 'VH' )

        #else:

            ##### do usual stuff #####
        
        return
            
            
            
            
#                     ######################### PT2 - ####################################
#                     ##### load PT2 schema #####
#                     schema = copy.deepcopy( self._pt2 ) 
#                     print( schema )
                    
#                     ##### set parameters of reader task #####
#                     param = self.getParameterSet( schema, 'Read' )
#                     param[ 'file' ] = outname_pt1 + '.dim'        # prev pt output as input
#                     param[ 'formatName' ] = 'SENTINEL-1'

#                     # create subset-specific output path
#                     param = self.getParameterSet ( schema, 'Write' )            
                    
#                     outname_pt2 = os.path.join( outname, 'subset_'+ subset_name + '_bnr_orb_cal_ml_tf')
#                     print( 'PT2 OUTNAME: ', outname_pt2 )
#                     param[ 'file' ] = outname_pt2
# #                     param[ 'file' ] = os.path.join( outname, outname_pt1 + 'subset_' + subset_name )
# #                     results.append( param['file'] ) # needs to be final output for each subset ############

#                     # transform dict back to xml schema & save serialised xml schema to file
#                     out = xmltodict.unparse( schema, pretty=True )
#                     cfg_pathname = os.path.join ( tmp_path, '{}.xml'.format( os.path.basename(outname_pt2) ) )
#                     with open( cfg_pathname, 'w+') as file:
#                         file.write(out)

#                     ##### execute PT1 processing for subset #####
#                     print ( 'Processing PT2 {} subset: {}'.format( hemisphere, subset_name ) )
#                     out, err, code = utility.execute( self._gpt, [ cfg_pathname ] )
#                     print ( f'OUT: { out }' )
#                     print ( f'ERR: { err }' )
#                     print ( '... OK!' )
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    

#         cmd = [snap_gpt, int_graph_1, f"-Pinput_grd={input_mani}", f"-Poutput_ml={inter_prod1}"]
#         root.info(cmd)
#         run_snap_command(cmd)
#         root.info(f"{in_scene} {scene_name} PROCESSED to MULTILOOK starting PT2")
                
#         if not os.path.exists(out_prod1):
#             if ext_dem:
#                 s3_download(s3_bucket, ext_dem, ext_dem_path)  # inc. function to subset by S1 scene extent on fly due to cog
#                 cmd = [snap_gpt, int_graph_2, f"-Pinput_ml={inter_prod1}", f"-Pext_dem={ext_dem_path}", f"-Poutput_tf={inter_prod2}"]
#                 root.info(cmd)
#                 run_snap_command(cmd)
#                 root.info(f"{in_scene} {scene_name} PROCESSED to TERRAIN FLATTEN starting PT3")


#                 cmd = [snap_gpt, int_graph_3, f"-Pinput_tf={inter_prod2}", f"-Pext_dem={ext_dem_path}", f"-Poutput_db={out_prod1}"]
#                 root.info(cmd)
#                 run_snap_command(cmd)
#                 root.info(f"{in_scene} {scene_name} PROCESSED to dB starting PT4")

#                 cmd = [snap_gpt, int_graph_4, f"-Pinput_tf={inter_prod2}", f"-Pext_dem={ext_dem_path}", f"-Poutput_ls={out_prod2}"]
#                 root.info(cmd)
#                 run_snap_command(cmd)
#                 root.info(f"{in_scene} {scene_name} PROCESSED to lsm starting COG conversion")
                    
                    
                    
                    
                    




    def getArguments( self, args ):

        """
        parse supplied arguments or setup defaults
        """

        if args:

            # copy args if passed to constructor
            self._remove_border_noise = args.remove_border_noise
            self._remove_thermal_noise = args.remove_thermal_noise
            self._terrain_flattening = args.terrain_flattening
            self._geocoding = args.geocoding
            self._polarizations = args.polarizations
            self._target_resolution = args.target_resolution
            self._external_dem = args.external_dem
            self._scaling = args.scaling

        else:

            # default values
            self._remove_border_noise = True
            self._remove_thermal_noise = True
            self._terrain_flattening = True
            self._geocoding = 'Range-Doppler'
            self._polarizations = ['VV', 'VH' ]
            self._target_resolution = 20.0
            self._external_dem = None
            self._scaling = 'db'

        return


    def getTask ( self, schema, name ):

        """
        get task sub-schema
        """

        # locate node schema corresponding to name
        node = None

        for obj in schema[ 'graph' ][ 'node' ]:
            if obj [ '@id' ] == name: 
                node = obj.copy()

        return node


    def getParameterSet ( self, schema, name ):

        """
        get parameter-set within task schema
        """

        # locate parameter schema corresponding to task node
        node = None

        obj = self.getTask( schema, name )
        if obj is not None:
            node = obj[ 'parameters' ]

        return node


    def insertNewTask ( self, schema, name, after=None ):

        """
        insert new task into pipeline schema
        """

        # get xml schema for new task
        with open ( os.path.join( './utils/s1am/recipes/nodes', name + '.xml'  )) as fd:
            new_task = xmltodict.parse( fd.read() )[ 'node' ]

        # create new ordered dict 
        update = copy.deepcopy( schema )
        update[ 'graph' ][ 'node' ].clear()
 
        # copy nodes into deep copy 
        nodes = []; last_task = None
        for obj in schema[ 'graph' ][ 'node' ]:

            # insert new task
            if last_task is not None:
                if after is not None and last_task[ 'operator' ] == after: 
                    nodes.append( new_task )

            nodes.append( obj ); last_task = obj

        # add to list end
        if after is None:
            nodes.append( new_task[ 'node' ] )

        # update source product values 
        prev_task = None
        for obj in nodes:

            if prev_task is not None:
                obj[ 'sources' ] = OrderedDict ( [ ( 'sourceProduct', OrderedDict( [ ( '@refid', prev_task[ 'operator' ] ) ] ) ) ] )
                
            prev_task = obj
        
        # add nodes to updated ordered dict
        update[ 'graph' ][ 'node' ] = nodes
        return update


#     def buildSchema ( self, schema, meta ):

#         """
#         initialise pipeline configuration schema
#         """

#         #### optionally insert border noise removal #####
#         if self._remove_border_noise:

#             insert task and update parameters
#             schema = self.insertNewTask( schema, 'Remove-GRD-Border-Noise', after='Read' )
#             param = self.getParameterSet ( schema, 'Remove-GRD-Border-Noise' )
#             param['selectedPolarisations'] = ','.join( self._polarizations )

#         #### optionally insert thermal noise removal #####
#         if self._remove_thermal_noise:

#             schema = self.insertNewTask( schema, 'ThermalNoiseRemoval', after='Read' )
#             param = self.getParameterSet ( schema, 'ThermalNoiseRemoval' )
#             param['selectedPolarisations'] = ','.join( self._polarizations )

#         #### update arguments for calibration task #####
#         param = self.getParameterSet ( schema, 'Calibration' )
#         param['selectedPolarisations'] = ','.join( self._polarizations )
#         param['sourceBands'] = ','.join ( ['Intensity_' + x for x in self._polarizations ] )

#         #### optionally insert terrain flattening #####
#         if self._terrain_flattening:

#             schema = self.insertNewTask( schema, 'Terrain-Flattening', after='Calibration' )
#             param = self.getParameterSet ( schema, 'Terrain-Flattening' )

# #             param['sourceBands'] = ','.join ( ['Beta0_' + x for x in self._polarizations ] )
# #             param['reGridMethod'] = True if self._external_dem is None else False
#             pred_tc = 'Terrain-Flattening'

#         else:

#             # update calibration output bands
#             param['outputBetaBand'] = False
#             param['outputGammaBand'] = True
#             pred_tc = 'Calibration'

#         #### insert terrain correction task #####
#         if self._geocoding == 'Range-Doppler':

#             # range doppler
#             schema = self.insertNewTask( schema, 'Terrain-Correction', after=pred_tc )
#             param = self.getParameterSet ( schema, 'Terrain-Correction' )
# #             param['sourceBands'] = ','.join( ['Gamma0_' + x for x in self._polarizations ] )

#         elif self._geocoding == 'Simulation-Cross-Correlation':

#             # simulation cross correlation
#             schema = self.insertNewTask( schema, 'SAR-Simulation', after=pred_tc )
#             schema = self.insertNewTask( schema, 'Cross-Correlation', after='SAR-Simulation' )
#             schema = self.insertNewTask( schema, 'SARSim-Terrain-Correction', after='Cross-Correlation' )

#             param = self.getParameterSet ( schema, 'SAR-Simulation' )
# #             param['sourceBands'] = ','.join( [ 'Gamma0_' + x for x in self._polarizations ] )

#         else:

#             # invalid geocoding configuration
#             raise ValueError ( 'Invalid geocoding configuration {}'.format( geocoding ) )

#         ##### insert multilooking task #####
#         schema = self.insertNewTask( schema, 'Multilook', after='Calibration' )
#         looks = self.getMultiLookParameters( meta )

#         param = self.getParameterSet ( schema, 'Multilook' )
#         param['nRgLooks'] = looks[ 'range' ]
#         param['nAzLooks'] = looks[ 'azimuth' ]
        
#         # set up source bands
#         cal_param = self.getParameterSet ( schema, 'Calibration' )
# #         if cal_param['outputBetaBand'] == 'true':
# #             param['sourceBands'] = ','.join( ['Beta0_' + x for x in self._polarizations ] )

# #         elif cal_param['outputGammaBand'] == 'true':
# #             param['sourceBands'] = ','.join( ['Gamma0_' + x for x in self._polarizations ] )

#         ##### insert unit conversion task #####
#         if self._scaling in ['dB', 'db']:
#             source = 'Terrain-Correction' if self._geocoding == 'Range-Doppler' else 'SARSim-Terrain-Correction'

#             schema = self.insertNewTask( schema, 'LinearToFromdB', after=source )            
#             param = self.getParameterSet ( schema, 'LinearToFromdB' )
# #             param['sourceBands'] = ','.join( ['Gamma0_' + x for x in self._polarizations ] )

#         ##### write task #####
#         param = self.getParameterSet ( schema, 'Write' )
#         param['formatName'] = 'BEAM-DIMAP'

#         ##### TODO - intermediate products #####
#         ##### TODO - dem configuration #####
#         ##### TODO - interpolation methods #####


#         return schema


#     def getMultiLookParameters ( self, meta ):

#         """
#         convert target resolution into looks
#         """

#         looks = {}

#         # pixel spacing and target spatial resolution
#         sp_range = meta[ 'pixel_spacing' ][ 'range' ]
#         sp_azimuth  = meta[ 'pixel_spacing' ][ 'azimuth' ]

#         tr_range = self._target_resolution
#         tr_azimuth = self._target_resolution

#         # handle slant range
#         if meta[ 'projection' ] == 'Slant Range':

#             # compute ground range resolution and range looks
#             gr_ps = sp_range / ( math.sin( math.radians( meta[ 'incidence_mid_swath' ] ) ) )
#             looks[ 'range' ] = int(math.floor( float( tr_range ) / gr_ps ) )

#         elif meta[ 'projection' ] == 'Ground Range':

#             # compute range looks
#             looks[ 'range' ] = int(math.floor( float(tr_range) / sp_range ) )

#         else:
#             raise ValueError( 'Invalid parameter value : {}'.format( meta[ 'projection' ] ) )

#         # compute the azimuth looks
#         looks[ 'azimuth' ] = int(math.floor(float(tr_azimuth) / sp_azimuth))

#         # set the look factors to 1 if they were computed to be 0
#         looks[ 'range' ] = looks[ 'range' ] if looks[ 'range' ] > 0 else 1
#         looks[ 'azimuth' ] = looks[ 'azimuth' ] if looks[ 'azimuth' ] > 0 else 1

#         return looks


    def getOutName( self, schema_part, meta ):

        """
        derive file identifier from pipeline / dataset
        """
            
        Lut = { 'pt0': 'bnr_orb_cal_ml_tf_tc_db',
                'pt1': 'bnr_orb_cal_ml',
                'pt2': 'bnr_orb_cal_ml_tf',
                'pt3': 'bnr_orb_cal_ml_tf_tc_db',
                'pt4': 'bnr_orb_cal_ml_tf_tc_ls'
              }        
        product = meta[ 'product' ]

        nm = 'S1{}_{}_{}_{}'.format(  product[ 'satellite' ], 
                                        product[ 'mode' ],
                                        meta[ 'acquisition' ][ 'start' ].strftime( '%y%m%dT%H%M%S' ),
                                        Lut[ schema_part ] )
        print( f'OUTNAME - {schema_part}: {nm}' )
        return nm
        
        
#         def stringifyPipeline( schema ):

#             """
#             create operator shortname string tokenised by underscores
#             """

#             # operator shortnames
#             Lut = { 'Remove-GRD-Border-Noise': 'bnr',
#                     'ThermalNoiseRemoval': 'tnr',
#                     'Apply-Orbit-File': 'Orb',
#                     'Calibration': 'Cal',
#                     'Multilook': 'ML',
#                     'Terrain-Flattening': 'TF',
#                     'Terrain-Correction': 'TC',
#                     'LinearToFromdB' : 'db' }

#             ops = []

#             # return op shortnames separated with underscore
#             for obj in schema[ 'graph' ][ 'node' ]:

#                 if obj[ 'operator' ] in Lut:
#                     ops.append( Lut[ obj[ 'operator' ] ] )

#             return '_'.join( ops )            

#         # append dataset parameters to op shortname string
#         product = meta[ 'product' ]
#         return 'S1{}_{}_{}_{}'.format(  product[ 'satellite' ], 
#                                         product[ 'mode' ],
#                                         meta[ 'acquisition' ][ 'start' ].strftime( '%y%m%dT%H%M%S' ),
#                                         stringifyPipeline( schema ) )


    def getSceneExtent( self, meta ):

        """
        determine scene bounding box in geographic coordinates
        """

        # initialise min / max 
        min_lon = 1e10; min_lat = 1e10
        max_lon = -1e10; max_lat = -1e10

        # each point in meta coordinates
        for pt in meta[ 'aoi' ]:

            min_lat = min ( min_lat, pt[ 0 ] )
            min_lon = min ( min_lon, pt[ 1 ] )

            max_lat = max ( max_lat, pt[ 0 ] )
            max_lon = max ( max_lon, pt[ 1 ] )

        # return limits
        return { 'lon' : { 'min' : min_lon, 'max' : max_lon },
                 'lat' : { 'min' : min_lat, 'max' : max_lat } }


    def splitGcps( self, gcps ):

        """
        sort gcps crossing antemeridian into list of lists ordered by row
        """

        # create dictionary for result
        obj = { 'west' : [[]], 
                'east' : [[]] }

        # for each gcp in geolocation grid
        prev_row = 0
        for gcp in gcps:

            # create new list for new gcp line
            if gcp.GCPLine != prev_row:
                obj[ 'west' ].append( [] ); obj[ 'east' ].append( [] )
                prev_row = gcp.GCPLine

            # append to list dependent on longitude signage
            if gcp.GCPX < 0.0:
                obj[ 'west' ][ -1 ].append( gcp )
            else:
                obj[ 'east' ][ -1 ].append( gcp )

        return obj


    def getSubset( self, gcps, block  ):

        """
        get interpolation safe subset dimensions
        """

        def getLineRange( gcps, block ):

            """
            get row range
            """

            # get geolocation grid lines encompassing block
            lines = {}; prev_row = 0
            for idx, gcp_row in enumerate( gcps ):

                if gcp_row[ 0 ].GCPLine > block[ 'start' ] and 'min' not in lines:
                    lines[ 'min' ] = idx - 1

                if gcp_row[ 0 ].GCPLine > block[ 'end' ] and 'max' not in lines:
                    lines[ 'max' ] = idx

            if 'max' not in lines:
                lines[ 'max' ] = len( gcps ) - 1

            return lines

        # geolocation grid extent
        lines = getLineRange( gcps, block )

        subset = {  'y1' : block[ 'start' ],
                    'y2' : block[ 'end' ] }

        for idx in range( lines[ 'min' ], lines[ 'max' ] + 1 ):

            # from rightmost column furthest from meridian
            if int( gcps[ idx ][ 0 ].GCPPixel ) == 0:

                # find leftmost gcp column within line range
                if 'x2' not in subset:
                    subset[ 'x2' ] = int ( gcps[ idx ][ -2 ].GCPPixel )

                subset[ 'x2' ] = int ( min( subset[ 'x2' ], gcps[ idx ][ -2 ].GCPPixel ) )

            else:

                # find leftmost column furthest from meridian
                if 'x1' not in subset:
                    subset[ 'x1' ] = int ( gcps[ idx ][ 1 ].GCPPixel )

                subset[ 'x1' ] = int( max( subset[ 'x1' ], gcps[ idx ][ 1 ].GCPPixel ) )

        # define remaining subset coordinates
        if 'x1' not in subset: 
            subset[ 'x1' ] = 0.0

        if 'x2' not in subset: 
            subset[ 'x2' ] = ( block[ 'samples' ] - 1 ) - subset[ 'x1' ]

        return [ subset[ 'x1' ], subset[ 'y1' ], subset[ 'x2' ], subset[ 'y2' ] - subset[ 'y1' ] ]


    def generateImage( self, out_path, results, pol ):

        """
        combine subset output images into single mosaic 
        """
       
        # find subset images
        images = []
        for result in results:

            files = list( Path( result ).rglob( '*{}*.img'.format( pol ) ) )
            if len( files ) == 1:
                images.append( str ( files[ 0 ] ) )

        # use gdal warp to create mosaic
        kwargs = { 'format': 'GTiff', 'srcNodata' : 0.0, 'dstSRS' : 'epsg:3460' }
        pathname = os.path.join( out_path, 'Gamma0_{}_db.tif'.format( pol ) )

        ds = gdal.Warp( pathname, images, **kwargs )
        del ds

        return


