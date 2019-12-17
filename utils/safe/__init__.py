import logging
from datetime import datetime

import gdal
import xmltodict


def get_manifest(manifest_path):
    """
    load metadata from scene manifest file
    """

    # get xml schema for new task
    meta = {}

    with open(manifest_path) as fd:
        doc = xmltodict.parse(fd.read())

    # product characteristics
    meta['product'] = {'type': find_items(doc, 's1sarl1:productType')[0],
                       'class': find_items(doc, 's1sarl1:productClass')[0],
                       'satellite': find_items(doc, 'safe:number')[0],
                       'mode': find_items(doc, 's1sarl1:mode')[0]}

    # acquisition period
    nodes = find_items(doc, 'safe:acquisitionPeriod')
    meta['acquisition'] = {'start': datetime.strptime(nodes[0]['safe:startTime'], '%Y-%m-%dT%H:%M:%S.%f'),
                           'stop': datetime.strptime(nodes[0]['safe:stopTime'], '%Y-%m-%dT%H:%M:%S.%f')}

    # software identity
    nodes = find_items(doc, 'safe:software')
    meta['software'] = {'name': nodes[0]['@name'], 'version': nodes[0]['@version']}

    # get orbit numbers
    nodes = find_items(doc, 'safe:orbitNumber')
    meta['orbit_number'] = {'start': int(nodes[0][0]['#text']), 'stop': int(nodes[0][1]['#text'])}

    # get relative orbit numbers
    nodes = find_items(doc, 'safe:relativeOrbitNumber')
    meta['relative_orbit_number'] = {'start': int(nodes[0][0]['#text']), 'stop': int(nodes[0][1]['#text'])}

    # get orbit direction
    meta['orbit_direction'] = find_items(doc, 's1:pass')[0]

    # get scene coordinates
    nodes = find_items(doc, 'gml:coordinates')
    tuples = nodes[0].split(' ')

    # convert to float array
    meta['aoi'] = []
    for t in tuples:
        meta['aoi'].append([float(i) for i in t.split(',')])

    # get cycle number
    nodes = find_items(doc, 'safe:cycleNumber')
    meta['cycle_number'] = int(nodes[0])

    # get mission take id
    nodes = find_items(doc, 's1sarl1:missionDataTakeID')
    meta['mission_take_id'] = int(nodes[0])

    # get polarization channels
    nodes = find_items(doc, 's1sarl1:transmitterReceiverPolarisation')
    meta['polarization'] = nodes[0]

    # get slice number
    nodes = find_items(doc, 's1sarl1:sliceNumber')
    meta['slice_number'] = int(nodes[0])

    # get total slices
    nodes = find_items(doc, 's1sarl1:totalSlices')
    meta['total_slices'] = int(nodes[0])

    return meta


def find_items(obj, field):
    """
    recursively extract key values from dictionary
    """

    # for all key value pairs
    values = []
    for key, value in obj.items():

        # record value of key match
        if key == field:
            values.append(value)

        # recursive call on nested dict
        elif isinstance(value, dict):
            results = find_items(value, field)
            for result in results:
                values.append(result)

        # loop through contents in array
        elif isinstance(value, list):
            for item in value:

                # recursive call on nested dict
                if isinstance(item, dict):
                    results = find_items(item, field)
                    for result in results:
                        values.append(result)

    return values


def get_scene_extent(meta):

    """
    determine scene bounding box in geographic coordinates
    """

    # initialise min / max
    min_lon = 1e10
    min_lat = 1e10
    max_lon = -1e10
    max_lat = -1e10

    # each point in meta coordinates
    for pt in meta['aoi']:
        min_lat = min(min_lat, pt[0])
        min_lon = min(min_lon, pt[1])

        max_lat = max(max_lat, pt[0])
        max_lon = max(max_lon, pt[1])

    # return limits
    return {'lon': {'min': min_lon, 'max': max_lon},
            'lat': {'min': min_lat, 'max': max_lat}}


def get_subset(gcps, block):

    """
    get interpolation safe subset dimensions
    """

    def get_line_range(gcps, block):

        """
        get row range
        """

        # get geolocation grid lines encompassing block
        lines = {}
        prev_row = 0
        for idx, gcp_row in enumerate(gcps):

            if gcp_row[0].GCPLine > block['start'] and 'min' not in lines:
                lines['min'] = idx - 1

            if gcp_row[0].GCPLine > block['end'] and 'max' not in lines:
                lines['max'] = idx

        if 'max' not in lines:
            lines['max'] = len(gcps) - 1

        return lines

    # geolocation grid extent
    lines = get_line_range(gcps, block)

    subset = {'y1': block['start'],
              'y2': block['end']}

    for idx in range(lines['min'], lines['max'] + 1):

        # from rightmost column furthest from meridian
        if int(gcps[idx][0].GCPPixel) == 0:

            # find leftmost gcp column within line range
            if 'x2' not in subset:
                subset['x2'] = int(gcps[idx][-2].GCPPixel)

            subset['x2'] = int(min(subset['x2'], gcps[idx][-2].GCPPixel))

        else:

            # find leftmost column furthest from meridian
            if 'x1' not in subset:
                subset['x1'] = int(gcps[idx][1].GCPPixel)

            subset['x1'] = int(max(subset['x1'], gcps[idx][1].GCPPixel))

    # define remaining subset coordinates
    if 'x1' not in subset:
        subset['x1'] = 0.0

    if 'x2' not in subset:
        subset['x2'] = (block['samples'] - 1) - subset['x1']

    logging.debug(f"{subset['x1']},{subset['y1']},{subset['x2']},{subset['y2'] - subset['y1']}")

    return f"{subset['x1']},{subset['y1']},{subset['x2']},{subset['y2'] - subset['y1']}"


def split_gcps(gcps):

    """
    sort gcps crossing antemeridian into list of lists ordered by row
    """

    # create dictionary for result
    obj = {'west': [[]],
           'east': [[]]}

    # for each gcp in geolocation grid
    prev_row = 0
    for gcp in gcps:

        # create new list for new gcp line
        if gcp.GCPLine != prev_row:
            obj['west'].append([])
            obj['east'].append([])
            prev_row = gcp.GCPLine

        # append to list dependent on longitude signage
        if gcp.GCPX < 0.0:
            obj['west'][-1].append(gcp)
        else:
            obj['east'][-1].append(gcp)

    return obj


def get_annotation(annotation):
    """
    load metadata from scene annotation file
    """

    # get xml schema for new task
    meta = {}

    with open(annotation) as fd:
        doc = xmltodict.parse(fd.read())

    # get resolution
    meta['pixel_spacing'] = {'range': float(find_items(doc, 'rangePixelSpacing')[0]),
                             'azimuth': float(find_items(doc, 'azimuthPixelSpacing')[0])}

    # get scene dimensions
    meta['image'] = {'samples': int(find_items(doc, 'numberOfSamples')[0]),
                     'lines': int(find_items(doc, 'numberOfLines')[0])}

    # get viewing geometry
    meta['projection'] = find_items(doc, 'projection')[0]
    meta['incidence_mid_swath'] = float(find_items(doc, 'incidenceAngleMidSwath')[0])

    # get heading
    meta['heading'] = float(find_items(doc, 'platformHeading')[0])
    if meta['heading'] < 0.0:
        meta['heading'] = meta['heading'] + 360.0

    return meta


def get_geolocation_grid(annotation):
    """
    load metadata from scene annotation file
    """

    # get xml schema for new task
    meta = {}

    with open(annotation) as fd:
        doc = xmltodict.parse(fd.read())

    # get gcps
    meta['gcps'] = []
    for pt in doc['product']['geolocationGrid']['geolocationGridPointList']['geolocationGridPoint']:
        # parse meta fields into gdal GCP object
        gcp = gdal.GCP(float(pt['longitude']),
                       float(pt['latitude']),
                       float(pt['height']),
                       float(pt['pixel']),
                       float(pt['line']))

        meta['gcps'].append(gcp)

    return meta

