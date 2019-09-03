from osgeo import ogr, osr
from shapely import wkt
import geopandas as gpd


def reproject_wkt_4326_3460(wkt):
    """
    Reproject wkt geometry from wgs84 into Fiji local coordinate system

    :param bands: download only a subset of S2 bands. default is False. input is list i.e. [B02.jp2, B03.jp2]
    :return:
    
    TO DO:UPDATE PARAMS
    """
    
    source = osr.SpatialReference()
    source.ImportFromEPSG(4326)

    target = osr.SpatialReference()
    target.ImportFromEPSG(3460)

    transform = osr.CoordinateTransformation(source, target)

    polygon = ogr.CreateGeometryFromWkt(str(wkt))
    polygon.Transform(transform)

    return polygon.ExportToWkt()


def reproFijiGDF(in_gdf):
    """
    Reproject gdf from wgs84 into Fiji local coordinate system

    :param bands: input geodataframe with geometry set as geometry
    :return:
    
    TO DO:UPDATE PARAMS
    """
    in_gdf['repro'] = in_gdf['geometry'].apply(lambda x: reproject_wkt_4326_3460(str(x)))
    in_gdf['repro'] = in_gdf['repro'].apply(wkt.loads)
    in_gdf = gpd.GeoDataFrame(in_gdf, geometry='repro', crs=('epsg:3460'))

    return in_gdf