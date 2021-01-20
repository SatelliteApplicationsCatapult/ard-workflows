import os
import xmltodict

from osgeo import gdal
from datetime import datetime
from . utility import findItems

def getManifest( pathname ):

    """
    load metadata from scene manifest file
    """

    # get xml schema for new task
    meta = {}

    with open ( pathname ) as fd:
        doc = xmltodict.parse( fd.read() )

    # product characteristics
    meta[ 'product' ] = {   'type' : findItems( doc, 's1sarl1:productType' )[ 0 ], 
                                'class' : findItems( doc, 's1sarl1:productClass' )[ 0 ], 
                                   'satellite' : findItems( doc, 'safe:number' )[ 0 ],
                                       'mode' : findItems( doc, 's1sarl1:mode' )[ 0 ] }

    # acquisition period
    nodes = findItems( doc, 'safe:acquisitionPeriod' )
    meta[ 'acquisition' ] = { 'start' : datetime.strptime( nodes[0][ 'safe:startTime' ], '%Y-%m-%dT%H:%M:%S.%f' ),
                                'stop' : datetime.strptime( nodes[0][ 'safe:stopTime' ], '%Y-%m-%dT%H:%M:%S.%f' ) }

    # software identity
    nodes = findItems( doc, 'safe:software' )
    meta[ 'software' ] = { 'name' : nodes[ 0 ][ '@name' ], 'version' : nodes[ 0 ][ '@version' ] }

    # get orbit numbers
    nodes = findItems( doc, 'safe:orbitNumber' )
    meta[ 'orbit_number' ] = { 'start' : int ( nodes[ 0 ][ 0 ][ '#text' ] ), 'stop' : int ( nodes[ 0 ][ 1 ][ '#text' ] ) }

    # get relative orbit numbers
    nodes = findItems( doc, 'safe:relativeOrbitNumber' )
    meta[ 'relative_orbit_number' ] = { 'start' : int ( nodes[ 0 ][ 0 ][ '#text' ] ), 'stop' : int ( nodes[ 0 ][ 1 ][ '#text' ] ) }

    # get orbit direction
    meta[ 'orbit_direction' ] = findItems( doc, 's1:pass' )[ 0 ]

    # get scene coordinates
    nodes = findItems( doc, 'gml:coordinates' )
    tuples = nodes[ 0 ].split( ' ' )

    # convert to float array
    meta[ 'aoi' ] = []
    for t in tuples:
        meta[ 'aoi' ].append( [ float( i ) for i in t.split( ',' ) ] )

    # get cycle number
    nodes = findItems( doc, 'safe:cycleNumber' )
    meta[ 'cycle_number' ] = int ( nodes[ 0 ] )

    # get mission take id
    nodes = findItems( doc, 's1sarl1:missionDataTakeID' )
    meta[ 'mission_take_id' ] = int ( nodes[ 0 ] )

    # get polarization channels
    nodes = findItems( doc, 's1sarl1:transmitterReceiverPolarisation' )
    meta[ 'polarization' ] = nodes[ 0 ]

    # get slice number
    nodes = findItems( doc, 's1sarl1:sliceNumber' )
    meta[ 'slice_number' ] = int ( nodes[ 0 ] )

    # get total slices
    nodes = findItems( doc, 's1sarl1:totalSlices' )
    meta[ 'total_slices' ] = int ( nodes[ 0 ] )

    return meta


def getAnnotation( annotation ):

    """
    load metadata from scene annotation file
    """

    # get xml schema for new task
    meta = {}

    with open ( annotation  ) as fd:
        doc = xmltodict.parse( fd.read() )

    # get resolution
    meta[ 'pixel_spacing' ] = { 'range' : float( findItems( doc, 'rangePixelSpacing' )[ 0 ] ),
                                    'azimuth' : float( findItems( doc, 'azimuthPixelSpacing' )[ 0 ] ) }

    # get scene dimensions
    meta[ 'image' ] = { 'samples' : int( findItems( doc, 'numberOfSamples' )[ 0 ] ),
                            'lines' : int( findItems( doc, 'numberOfLines' )[ 0 ] ) }

    # get viewing geometry
    meta[ 'projection' ] = findItems( doc, 'projection' )[ 0 ]
    meta[ 'incidence_mid_swath' ] = float ( findItems( doc, 'incidenceAngleMidSwath' )[ 0 ] )

    # get heading
    meta[ 'heading' ] = float ( findItems( doc, 'platformHeading' )[ 0 ] )
    if meta[ 'heading' ] < 0.0: 
        meta[ 'heading' ] = meta[ 'heading' ] + 360.0

    return meta


def getGeolocationGrid( annotation ):

    """
    load metadata from scene annotation file
    """

    # get xml schema for new task
    meta = {}

    with open ( annotation  ) as fd:
        doc = xmltodict.parse( fd.read() )

    # get gcps
    meta[ 'gcps' ] = []
    for pt in doc[ 'product' ][ 'geolocationGrid' ][ 'geolocationGridPointList' ][ 'geolocationGridPoint' ]:

        # parse meta fields into gdal GCP object
        gcp = gdal.GCP( float( pt[ 'longitude' ] ),
                        float( pt[ 'latitude' ] ),
                        float( pt[ 'height' ] ),
                        float( pt[ 'pixel' ] ),
                        float( pt[ 'line' ] ) )

        meta[ 'gcps' ].append ( gcp )

    return meta


