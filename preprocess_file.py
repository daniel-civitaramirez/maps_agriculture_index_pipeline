import os
import fiona
import pandas as pd
import geopandas as gpd
from pathlib import Path

from fiona.crs import from_epsg

gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'

class ProcessMultiPoly(object):
    '''
    Overview: Set of functions that process MultiPoly geometry in geopandas dataframe.
    '''

    def check_type(self, poly, poly_type):
        '''
        Check if polygon is of type informed in poly_type

        Inputs
            poly: shapely geometry class entity 
                polygon which will be tested.
            poly: str
                expected polygon type 
            
        Outputs
            boolean: if True the polygon has supposed type; if False polygon has other type than supposed type.
            
        '''
    
        if poly.geom_type == poly_type:
            return True
        else:
            return False

    def _row_by_type(self, gdf, poly_type):
        '''
        Return entries of geodataframe that correspond the expected polygon type; otherwise, if none geopandas line has expected type, returns none.

        Inputs
            gdf: geopandas dataframe
                geopandas dataframe with entries that will be filtered.
            poly_type: str
                type used for filter information of geopandas.
            
        Outputs
            geopandas only with entries of expected polygon, otherwise None.
            
        '''
        if not gdf[gdf.apply(lambda x: self.check_type(x.geometry, poly_type), axis=1)].empty:
            return gdf[gdf.apply(lambda x: self.check_type(x.geometry, poly_type), axis=1)]
        else:
            return None

    def _transform_multipoly(self, multipoly):
        '''
        Transform a geopandas composed by MultiPolygons into a geopandas where each line represents these aggreated MultiPolygons.

        Inputs
            gdf: geopandas dataframe 
                geopandas dataframe with MultiPolygon entries to be processed.
            
        Outputs
            geopandas with expanded Polygons retrived from MultiPolygons lines.
            
        '''
        tmp_list = []
        if len(multipoly) > 1:
            for idx, row in multipoly.iterrows():
                if len(list(row.geometry))>1:
                    for k, p in enumerate(row.geometry):
                        tmp_list.append({
                            'geometry' : p,
                            'name': F'{row["name"]}_{k}',
                        })
                else:
                    tmp_list.append({
                        'geometry' : list(row.geometry)[0],
                        'name': F'{row["name"]}',
                    })
            return gpd.GeoDataFrame(tmp_list)
        else:
            multipoly["geometry"] = list(multipoly["geometry"].iloc[0])[0]
            return multipoly

    def preprocess_multipoly(self, gdf):
        '''
        Preprocess a geopandas dataframe composed by both Polygons and MultiPolygons

        Inputs
            gdf: geopandas dataframe 
                geopandas with Polygons and MultiPolygons to be processed.
            
        Outputs
            geopandas only with Polygons: those which already were in geopandas dataframe and new ones processed from MultiPolygons.
            
        '''
        multipoly_df = self._row_by_type(gdf, 'MultiPolygon')
        poly_df = self._row_by_type(gdf, 'Polygon')
   
        if (multipoly_df is None) and (poly_df is None):
            raise Exception('None Polygon was found. Data type not recognized.')

        if (not multipoly_df is None):
            poly_df_processed = self._transform_multipoly(multipoly_df)
            return pd.concat([poly_df_processed, poly_df])
        else:
            return poly_df
    
    def _rename_geopandas(self, gdf, filepath):
        '''
        Given filepath name, it uses this name to rename the lines of geopandas dataframe
        '''
        name_file = os.path.basename(Path(filepath))
        name_file = name_file[:name_file.rfind('.')]

        if "Name" in gdf.columns:
            gdf.rename(columns={'Name':'name'}, inplace=True)
        
        if len(gdf) > 1 :
            gdf['name'] = [F'{name_file}_{x}' for x in range(len(gdf))]
        else:
            gdf['name'] = name_file
        return gdf

    def get_geopandas(self, filepath):
        '''
        Given a file path returns geopandas with processed MultiPolygons

        Inputs
            filepath: str
                
        Outputs
            geopandas only with Polygons: those which already were in geopandas dataframe and new ones processed from MultiPolygons.
            
        '''
        gdf = gpd.read_file(filepath)
        gdf = self._rename_geopandas(gdf, filepath)
        gdf = self.preprocess_multipoly(gdf)
        return gdf


class FileProcessing(object):
    '''
    Overview: Functions which read and process kml and shp files
    '''

    def __init__(self, crs=4326):
        self.crs = crs
    
    def _get_filepathname(self, filepath):
        filename = os.path.basename(Path(filepath))
        filename = filename[:filename.rfind('.')]
        return filename

    def read_from_kml(self, filepath):
        '''
        Given a file path for kml file returns geopandas with information within the file.

        Inputs
            filepath: str
                
        Outputs
            geopandas with KML content
            
        '''
        df = gpd.GeoDataFrame(crs=from_epsg(self.crs))
        for idx,layer in enumerate(fiona.listlayers(filepath)):
            s = gpd.read_file(filepath, driver='KML', layer=layer)
            df = df.append(s, ignore_index=True)

        filename = self._get_filepathname(filepath)

        if len(df)>1:
            df['name'] = [f'{filename}_{x}' for x in range(0, len(df))]
        else:
            df['name'] = filename
        return df

    def read_from_shapefile(self, filepath):
        '''
        Given a file path for shapefile file returns geopandas with information within the file.

        Inputs
            filepath: str
                
        Outputs
            geopandas with shapefile content
            
        '''
        gdf = gpd.read_file(filepath)
        filename = self._get_filepathname(filepath)

        if len(gdf)>1:
            gdf['name'] = [f'{filename}_{x}' for x in range(0, len(gdf))]
        else:
            gdf['name'] = filename
        return gdf
    
    def read_from_geojson(self, filepath):
        '''
        Given a file path for geojson file returns geopandas with information within the file.

        Inputs
            filepath: str
                
        Outputs
            geopandas with geojson content    
        '''
        return gpd.read_file(filepath)
    
    def write_geojson(self, gdf, filename):
        '''
        Save content of geopandas dataframe into geojson file

        Inputs
            filepath: geopandas dataframe
                
        Outputs
            N/A
        '''
        return gdf.to_file(f'{filename}.geojson', driver='GeoJSON')

    def _get_filename(self, text):
        '''
        Given a string return substring contained by last . and final character (used for extraction of filename in path)
        '''
        end = text.rfind(".")
        return text[:end], text[end+1:]

    def _process_files(self, path, list_files):
        '''
        Given a path for files process all kml and shapefiles files within this path, and save respective geojson files for them.

        Inputs
            path: str
                path indicating where files will be processed. 
            
            list_files: list of str
                List of files to be processed.
                
        Outputs
            N/A
        '''
        for extension in ['kml', 'shp']:                
            list_files = list(filter(lambda k: extension == k[k.rfind('.')+1:], os.listdir(path)))

            for file_ in list_files:
                filename, ext = self._get_filename(file_)
                if ext == 'kml':
                    gdf = self.read_from_kml(os.path.join(path, file_))
                if ext == 'shp':
                    gdf = self.read_from_shapefile(os.path.join(path, file_))

                gdf = ProcessMultiPoly().preprocess_multipoly(gdf)
                self.write_geojson(gdf, os.path.join(path, filename))

    def create_geojson(self, base_path):
        '''
        Given a path, all files, and folder contained within this path which are kml and shapefile will be processed generating geojson files.

        Inputs
            path: str
                path indicating where folders and files will be processed. 
            
        Outputs
            N/A
        '''
        list_files_names = [f for f in os.listdir(base_path) if os.path.isfile(os.path.join(base_path,f))]
        list_folder_names = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path,f))]

        if list_files_names:
            self._process_files(base_path, list_files_names)
        
        if list_folder_names:
            for folder in list_folder_names:
                path = os.path.join(base_path, folder)
                list_files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))]
                self._process_files(path, list_files)
    
    