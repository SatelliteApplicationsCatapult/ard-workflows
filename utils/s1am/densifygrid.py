import os
import sys
import math
import shutil
import xmltodict
import collections
import numpy as np
import zipfile as zf
import matplotlib.pyplot as plt

from pathlib import Path
from threading import Thread
from datetime import datetime

from osgeo import gdal, osr
from pyproj import Proj, transform
from scipy.interpolate import griddata

import pdb

class DensifyGrid:

    def __init__( self ):

        """
        constructor
        """

        # metafield constants
        self._fields = [ 'azimuthTime', 'slantRangeTime', 'incidenceAngle', 'elevationAngle' ]
        self._proj = { 'latlon' : 'epsg:4326', 'mercator' : 'epsg:3460' }

        return


    def process( self, annotation_files, grid_pts=100, writeback=False ):

        """
        entry point into class functionality
        """

        # read annotation xml into dictionary
        for f in annotation_files:

            doc = self.readAnnotationFile( f )
            if doc is not None:

                # get scene image dimensions
#                 print ( 'Densifying geolocation grid in annotation file: {}'.format( f ) )
                dims = self.getDimensions( doc )

                # extract tie points from annotation schema and reproject to mercator
                gcps = { 'latlon' : self.getTiePoints( doc ) }
                gcps[ 'mercator' ] = self.reprojectTiePoints( gcps[ 'latlon' ], 
                                                                { 'source' : self._proj[ 'latlon' ], 'target' : self._proj[ 'mercator' ] } )

                # create denser tie point grid 
                x_grid, y_grid = self.getDenseGrid( dims, grid_pts )
                dense_grid = { 'pixel' : x_grid.flatten(), 'line' : y_grid.flatten() }              

                # interpolate parameter values onto dense grid
                dense_grid.update ( self.interpolateFields( doc, ( gcps[ 'latlon' ][ 'pixel' ], gcps[ 'latlon' ][ 'line' ] ), ( x_grid, y_grid ) ) )

                # interpolate gcps onto dense grid
                geo_transform = gdal.GCPsToGeoTransform( gcps[ 'mercator' ] )
#                 print ( '{}: Mean sum of squares: {} m '. format ( os.path.basename( f ), self.computeError( gcps[ 'mercator' ], geo_transform ) ) )

                dense_grid[ 'gcps' ] = self.interpolateTiePoints( geo_transform, dense_grid[ 'pixel' ], dense_grid[ 'line' ] )

                # optionally visualize dense grid map coordinates
                if writeback:
                    self.plotDenseGrid( dense_grid, grid_pts )

                # write denser tie point grid to updated annotation file and geotiff
                self.writeAnnotationFile( doc, dense_grid )
                # self.writeImageFile( doc, dense_grid )
#                 print ( '... OK!' )

        return


    def readAnnotationFile( self, pathname ):

        """
        read annotation xml schema into dictionary
        """

        # parse annotation files into dict
        doc = { 'pathname' : pathname }
        with open ( doc[ 'pathname' ] ) as fd:
            doc[ 'schema' ]=xmltodict.parse( fd.read() )

        return doc


    def getDimensions( self, doc ):

        """
        get scene dimensions
        """

        # extract samples and lines
        schema = doc[ 'schema' ]

        width = float ( schema[ 'product' ][ 'imageAnnotation' ][ 'imageInformation' ][ 'numberOfSamples' ] )
        height = float ( schema[ 'product' ][ 'imageAnnotation' ][ 'imageInformation' ][ 'numberOfLines' ] )

        return { 'width' : width, 'height' : height }


    def getTiePoints( self, doc ):

        """
        get tie points from annotation xml schema
        """

        # create dict for output
        gcps = {  'pixel' : np.asarray([]),
                    'line': np.asarray([]),
                    'X': np.asarray([]),
                    'Y': np.asarray([]),
                    'Z': np.asarray([]) }

        # load values from schema into data obj
        schema = doc[ 'schema' ]
        for pt in schema[ 'product' ][ 'geolocationGrid' ][ 'geolocationGridPointList' ][ 'geolocationGridPoint' ]:

            # parse values in numpy arrays
            gcps[ 'pixel' ]  = np.append( gcps[ 'pixel' ], float( pt[ 'pixel' ] ) )
            gcps[ 'line' ] = np.append( gcps[ 'line' ], float( pt[ 'line' ] ) )

            gcps[ 'X' ] = np.append( gcps[ 'X' ], float( pt[ 'longitude' ] ) )
            gcps[ 'Y' ] = np.append( gcps[ 'Y' ], float( pt[ 'latitude' ] ) )
            gcps[ 'Z' ] = np.append( gcps[ 'Z' ], float( pt[ 'height' ] ) )

        return gcps


    def getDenseGrid( self, dims, grid_pts ):

        """
        generate gcp mesh grid with customisable spacing
        """

        # create denser gcp mesh grid
        x = np.linspace(0, dims[ 'width' ], grid_pts, endpoint=False); 
        y = np.linspace(0, dims[ 'height' ], grid_pts, endpoint=False);

        X, Y = np.meshgrid(x, y, copy=False)

        return X, Y


    def interpolateFields( self, doc, grid_mesh, dense_grid_mesh ):

        """
        interpolate dense geometry and timing data
        """

        def getField( doc, field ):

            """
            transform meta parameter values into 1D numeric array
            """

            # retrieve tie point field data
            B = np.asarray([]); schema = doc[ 'schema' ]
            for pt in schema[ 'product' ][ 'geolocationGrid' ][ 'geolocationGridPointList' ][ 'geolocationGridPoint' ]:

                # convert time to epoch for interpolation
                if field == 'azimuthTime':
                    value = datetime.strptime( pt[ field ], '%Y-%m-%dT%H:%M:%S.%f' ).timestamp()
                else:
                    value = float( pt[ field ]  )

                # package up as 1d vector
                B = np.append( B, value )

            return B


        # view / timing parameters associated with each tie point
        result = {}
        for idx, field in enumerate( self._fields ):

            # 1d interpolation of tie-point field data onto denser grid
            B = getField( doc, field )
            Bi = griddata( grid_mesh, B, dense_grid_mesh, method='cubic' )

            result[ field ] = Bi.flatten()

        return result


    def interpolateTiePoints( self, geo_t, pixel, line ):

        """
        get geographic coordinates for nodes of denser grid using linear algebra
        """

        # create dict for output
        num_samples = len( pixel )
        gcps = {    'pixel' : np.zeros( num_samples ),
                    'line': np.zeros( num_samples ),
                    'X': np.zeros( num_samples ),
                    'Y': np.zeros( num_samples ),
                    'Z': np.zeros( num_samples ) }

        # compute simulated gcps across grid
        count = 0; 
        while count < num_samples:

            # compute interpolated map coordinates
            geo_x = geo_t[ 0 ] + pixel[ count ] * geo_t[ 1 ] + line[ count ] * geo_t[ 2 ]
            geo_y = geo_t[ 3 ] + pixel[ count ] * geo_t[ 4 ] + line[ count ] * geo_t[ 5 ]

            # record results for reprojection to geographic
            gcps[ 'X' ][ count ] = geo_x
            gcps[ 'Y' ][ count ] = geo_y
        
            gcps[ 'line' ][ count ] = line[ count ]
            gcps[ 'pixel' ][ count ] = pixel[ count ]

            count = count + 1

        return self.reprojectTiePoints( gcps, { 'source' : self._proj[ 'mercator' ], 'target' : self._proj[ 'latlon' ] } )


    def reprojectTiePoints( self, gcps, projection, threads=4 ):

        """
        reproject geographic coordinates to antemeridian friendly mercator SRS
        """

        def executeTask( task, gcps, projection, records ):

            """
            threading function for computing reprojection of point subset 
            """

            # get geographic and tm projection objects
            srs_s = Proj(init=projection[ 'source' ]); srs_t = Proj(init=projection[ 'target' ])
            gcps_warp = []            

            # process subset of gcp list
            idx = task[ 'offset' ]; num_gcps = len ( gcps[ 'pixel' ] ); count = 0
            while idx <= ( task[ 'offset' ] + task[ 'items' ] ) and idx < num_gcps :

                # reproject coordinates from source to target srs
                x,y,z = transform(  srs_s, 
                                    srs_t, 
                                    gcps[ 'X' ][ idx ], 
                                    gcps[ 'Y' ][ idx ], 
                                    gcps[ 'Z' ][ idx ] )

                gcps_warp.append( gdal.GCP( x, y, z, gcps[ 'pixel' ][ idx ], gcps[ 'line' ][ idx ] ) )
                idx = idx + 1; count = count + 1

            # copy to shared array
            records[ task[ 'index' ] ] = gcps_warp

            return


        def getTaskList( gcps ):

            """
            distribute reprojection of dense grid geographic coordinates across multiple threads
            """

            # determine optimal split
            samples = len( gcps[ 'pixel' ] )
            interval = int ( math.ceil ( samples / threads ) )

            # counters
            tasks = []; index = 0; next = 0
            while next < samples:

                # split gcp array into chunks
                tasks.append ( { 'index' : index, 'offset' : next, 'items' : interval } )

                next = next + interval + 1
                index = index + 1

            return tasks

        # get task list
        tasks = getTaskList( gcps )
        records = [ [] for t in range( threads ) ]

        # create thread per task
        threads = []
        for task in tasks:

            # We start one thread per url present.
            process = Thread(target=executeTask, args=[ task, gcps, projection, records ] )
            process.start()
            threads.append(process)

        # pause main thread until all child threads complete
        for process in threads:
            process.join()

        # flatten array of lists into single aggregated list
        return [ item for sublist in records for item in sublist ]


    def writeAnnotationFile( self, doc, dense_grid ):

        """
        write annotation xml file with denser tie point grid
        """   

        # clear geolocation grid schema from doc
        schema = doc[ 'schema' ]

        arr = schema[ 'product' ][ 'geolocationGrid' ][ 'geolocationGridPointList' ][ 'geolocationGridPoint' ]
        arr.clear()

        # rebuild geolocation grid schema
        count = 0
        while count < len( dense_grid[ 'gcps' ] ):

            obj = collections.OrderedDict()

            # view and timing info
            obj[ 'azimuthTime' ] = datetime.fromtimestamp( dense_grid[ 'azimuthTime' ][ count ] ).strftime (  '%Y-%m-%dT%H:%M:%S.%f' )
            obj[ 'slantRangeTime' ] = dense_grid[ 'slantRangeTime' ][ count ]
            obj[ 'incidenceAngle' ] = dense_grid[ 'incidenceAngle' ][ count ]
            obj[ 'elevationAngle' ] = dense_grid[ 'elevationAngle' ][ count ]

            # tie points
            obj[ 'pixel' ] = dense_grid[ 'gcps' ][ count ].GCPPixel
            obj[ 'line' ] = dense_grid[ 'gcps' ][ count ].GCPLine
            obj[ 'longitude' ] = dense_grid[ 'gcps' ][ count ].GCPX
            obj[ 'latitude' ] = dense_grid[ 'gcps' ][ count ].GCPY
            obj[ 'height' ] = dense_grid[ 'gcps' ][ count ].GCPZ

            arr.append( obj )
            count += 1

        # update tie point count attribute
        schema[ 'product' ][ 'geolocationGrid' ][ 'geolocationGridPointList' ][ '@count' ] = str(  len( dense_grid[ 'gcps' ] ) )

        # parse dict back into xml schema
        out = xmltodict.unparse( schema, pretty=True )

        # write serialized xml schema to file
        with open( str( doc[ 'pathname' ] ), 'w+') as file:
            file.write(out)

        return 


    def writeImageFile( self, doc, dense_grid ):

        """
        write scene image file with denser tie point grid
        """

        # open scene and extract gcps
        image_pathname = doc[ 'pathname' ].replace ( 'annotation', 'measurement' ).replace ( '.xml', '.tiff' )
        in_ds = gdal.Open( image_pathname, gdal.GA_Update )        

        in_ds.SetGeoTransform([0, 0, 0, 0, 0, 0])

        # close dataset
        in_ds.FlushCache() 
        in_ds = None

        # open scene and extract gcps
        in_ds = gdal.Open( image_pathname, gdal.GA_Update )        

        gcp_srs = osr.SpatialReference()
        gcp_srs.ImportFromEPSG( 4326 )
        gcp_crs_wkt = gcp_srs.ExportToWkt()

        in_ds.SetGCPs( dense_grid[ 'gcps' ], gcp_crs_wkt )

        # close dataset
        in_ds.FlushCache() 
        in_ds = None

        return


    def computeError( self, gcps, geo_t ):

        """
        compute RMS error between modelled and actual mercator coordinates
        """

        # compute simulated gcps across grid
        sum_error = 0; 
        for gcp in gcps:

            # compute interpolated map coordinates
            geo_x = geo_t[ 0 ] + gcp.GCPPixel * geo_t[ 1 ] + gcp.GCPLine * geo_t[ 2 ]
            geo_y = geo_t[ 3 ] + gcp.GCPPixel * geo_t[ 4 ] + gcp.GCPLine * geo_t[ 5 ]

            sum_error += math.sqrt (  ( gcp.GCPX - geo_x )**2 + ( gcp.GCPY - geo_y )**2 ) 
#             print ( geo_x, geo_y, gcp.GCPX, gcp.GCPY )

        return sum_error / len ( gcps )


    def plotDenseGrid( self, dense_grid, grid_pts ):

        """
        visualise interpolated datasets on dense grid
        """

        def getData( name ):

            data = np.zeros ( len ( dense_grid[ 'gcps' ] ) )
            for idx, gcp in enumerate( dense_grid[ 'gcps' ] ):
                data[ idx ] = gcp.GCPX if name == 'X' else gcp.GCPY

            return data

        # create figure
        fig = plt.figure()
        for idx, name in enumerate ( [ 'X', 'Y' ] ):
            
            # extract data and reshape
            data = np.reshape( getData( name ), ( grid_pts, grid_pts ) )

            x_grid = np.reshape( dense_grid[ 'pixel' ], ( grid_pts, grid_pts ) )
            y_grid = np.reshape( dense_grid[ 'line' ], ( grid_pts, grid_pts ) )

            ax = fig.add_subplot( 1, 2, idx + 1 )
            ax.contourf( x_grid, y_grid, data )
            ax.set_title( name )

        fig.tight_layout(); 
        plt.show()
        return

