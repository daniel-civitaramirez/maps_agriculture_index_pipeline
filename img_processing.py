import os
import zipfile
import itertools
import numpy as np
import pandas as pd
from datetime import date

from sentinelsat import SentinelAPI, geojson_to_wkt

import rasterio.mask
import rasterio as rio
from rasterio.warp import calculate_default_transform, reproject, Resampling

from fiona.crs import from_epsg

import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPoint, box
import shapely.wkt
import datetime

class ImageProcessing(object):
    '''
        This class is composed by a set of methods that generate NDRI and NDRE files and masks
        area of interest.

        Parameters
        ----------

        gdf: geodataframe
            geodataframe with the polygon of interest. 
        
        database: str, default='Sentinel_Data''
            path to database where Sentinel products are downloaded.
    '''

    def __init__(self, gdf, database='Sentinel_Data'):
        self.gdf = gdf
        self.database = database
        self.crs = 4326
        self. __int_directories()

    def __int_directories(self):
        '''
        Overview: creates directory with polygon name with NDVI,NDRE and TCI sub directories.
        '''
        for index,rows in self.gdf.iterrows():
            if not os.path.exists(rows['name']):
                filepath = os.path.join(rows['name'], 'NDVI')
                os.makedirs(filepath)
                filepath = os.path.join(rows['name'], 'NDRE')
                os.makedirs(filepath)
                filepath = os.path.join(rows['name'], 'TCI')
                os.makedirs(filepath)

    def get_bbox(self, polygon):
        '''
        Overview: creates a bbox of polygon.

        Inputs
            polygon: Shape
                shape of interest
        
        Output
            returns bbox of polygon.
        '''
        poly_bounds = polygon.bounds
        return box(poly_bounds[0], poly_bounds[1], poly_bounds[2], poly_bounds[3])

    def get_folders(self,csv_path,initial_date=None,final_date=None,threshold=0.97):
        '''
        Overview: returns list of folders and dates of interest for generating NDRE, NDVI and masking.

        Inputs
            csv_path: str
                path to csv file, filepath, that contains information on all downloaded products in current
                database.
            intial_date: str, default=None
                inital date of interest for getting folders for image generation and masking.
            final_date: str, default=None
                final date of interest for getting folders for image generation and masking.
            threshold: float, default=0.97
                the minimum area intersection required for getting folder for image generation and masking.
        
        Outputs
            folders: dictionary
                folders of interest for for image generation and masking.
            date: dictionary
                dates corresponding to folders; indicates date of the image. 
        '''
        df = pd.read_csv(csv_path)
        folders = {}
        date = {}
        df.ingestiondate = pd.to_datetime(df.ingestiondate)
        if initial_date is not None:
            intial_date = datetime.datetime.strptime(intial_date, '%d/%m/%Y')
            df = df[df.ingestiondate > initial_date]
        if final_date is not None:
            final_date = datetime.datetime.strptime(final_date, '%d/%m/%Y')+datetime.timedelta(days=1)
            df = df[df.ingestiondate < final_date]
        
        for gdf_idx,gdf_row in self.gdf.iterrows():
            folders[gdf_row['name']] = []
            date[gdf_row['name']] = []
            for idx, row in df.iterrows():
                senti_poly = shapely.wkt.loads(row.geometry)
                intersect_area = gdf_row.geometry.intersection(
                    senti_poly).area
                area = intersect_area / gdf_row['geometry'].area
                if area >= threshold:
                    folders[gdf_row['name']] += [row.filename]
                    date[gdf_row['name']] += [row.ingestiondate]
        return folders, date
    
    def get_img_path(self, folder):
        '''
        Overview: returns path to IMG_DATA folder of a specific product folder.

        Inputs
            folder: str
                name of folder of interest that is in the database.
        
        Output
            str with path to IMG_DATA folder that is inside the folder of interest.
        '''
        path = os.path.join(self.database, folder, 'GRANULE')
        f = [i for i in os.listdir(path)]
        path = os.path.join(path, f[0], 'IMG_DATA')
        return path

    def generate_ndvi(self, img_path):
        '''
        Overview: generates NDVI image file.

        Inputs
            img_path: str
                path to IMG_DATA of specific folder in the database where 
                NDVI file will be generated.
        
        Outputs
            N/A.
        '''
        path = os.path.join(img_path, 'R10m')
        if 'NDVI.tiff' not in os.listdir(path):
            bands = {}
            for f in os.listdir(path):
                if '.jp2' in f:
                    bands[f.split('_')[2]] = f
            filepath = os.path.join(path, bands['B04'])
            b4 = rio.open(filepath, driver='JP2OpenJPEG')
            filepath = os.path.join(path, bands['B08'])
            b8 = rio.open(filepath, driver='JP2OpenJPEG')

            red = b4.read()
            nir = b8.read()
            ndvi = (nir.astype(float)-red.astype(float))/(nir+red)
            meta = b4.meta
            meta.update(driver='GTiff')
            meta.update(dtype=rio.float32)
            filepath = os.path.join(path, 'NDVI.tiff')

            with rio.open(filepath, 'w', **meta) as dst:
                dst.write(ndvi.astype(rio.float32))

    def generate_ndre(self, img_path):
        '''
        Overview: generates NDRE image file.

        Inputs
            img_path: str
                path to IMG_DATA of specific folder in the database where 
                NDRE file will be generated.
        
        Outputs
            N/A.
        '''
        path = os.path.join(img_path, 'R10m')
        if 'NDRE.tiff' not in os.listdir(path):
            path = os.path.join(img_path, 'R20m')
            bands = {}
            for f in os.listdir(path):
                if '.jp2' in f:
                    bands[f.split('_')[2]] = f
            path = os.path.join(path, bands['B05'])
            b5 = rio.open(path, driver='JP2OpenJPEG')

            path = os.path.join(img_path, 'R10m')
            bands = {}
            for f in os.listdir(path):
                if '.jp2' in f:
                    bands[f.split('_')[2]] = f
            filepath = os.path.join(path, bands['B08'])

            with rasterio.open(filepath) as src:
                out_height = int(src.height * 0.5)
                out_width = int(src.width * 0.5)

                data = src.read(
                    out_shape=(src.count, out_height, out_width),
                    resampling=Resampling.bilinear
                )

                out_transform = src.transform * src.transform.scale(
                    (src.width / data.shape[-1]),
                    (src.height / data.shape[-2])
                )

                kwargs = src.meta.copy()
                kwargs.update({"driver": "GTiff", 'height': out_height,
                               'width': out_width, 'transfrom': out_transform})

                path = os.path.join(path, 'B8_reproj.tiff')
                with rasterio.open(path, 'w', **kwargs) as dst:
                    dst.write(data)

            path = os.path.join(img_path, 'R10m', 'B8_reproj.tiff')
            b8 = rio.open(path)

            rededge = b5.read()
            nir = b8.read()

            ndre = (nir.astype(float)-rededge.astype(float))/(nir+rededge)
            meta = b5.meta
            meta.update(driver='GTiff')
            meta.update(dtype=rio.float32)

            path = os.path.join(img_path, 'R10m', 'NDRE.tiff')
            with rio.open(path, 'w', **meta) as dst:
                dst.write(ndre.astype(rio.float32))

    def mask_ndre(self, img_path, date, poly_name=None):
        '''
        Overview: creates a clipped image of NDRE file for polygon of interest, polygon's NDRE folder.

        Inputs
            img_path: str
                path to IMG_DATA folder of a product folder in the databse for NDRE masking. 
            date: datetime
                date corresponding to product folder; date which the images of such folder where taken.
            poly_name: bool
                true if you want to select specific polygon; multipoly case.
        
        Output
            N/A.
        '''
        gdf = self.gdf.iloc[[0]]

        if poly_name != None:
            gdf = self.gdf[self.gdf.name == poly_name]
        
        date = date.strftime('%d-%m-%Y')
        filepath = os.path.join(img_path, 'R10m', 'NDRE.tiff')
        with rio.open(filepath) as src:
            shape_proj = gdf.to_crs(src.crs.data)
            out_image, out_transform = rasterio.mask.mask(
                src, shape_proj.geometry, crop=True)
            out_meta = src.meta.copy()
            out_meta.update(
                {"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})

        filepath = os.path.join(gdf.name.iloc[0], 'NDRE', date+'_ndre.tiff')
        with rio.open(filepath, 'w', **out_meta) as dst:
            dst.write(out_image)

    def mask_ndvi(self, img_path, date, poly_name=None):
        '''
        Overview: creates a clipped image of NDVI file for polygon of interest, polygon's NDVI folder.

        Inputs
            img_path: str
                path to IMG_DATA folder of a product folder in the databse for NDVI masking. 
            date: datetime
                date corresponding to product folder; date which the images of such folder where taken.
            poly_name: bool
                true if you want to select specific polygon; multipoly case.
        
        Output
            N/A.
        '''
        gdf = self.gdf.iloc[[0]]

        if poly_name != None:
            gdf = self.gdf[self.gdf.name == poly_name]

        date = date.strftime('%d-%m-%Y')
        filepath = os.path.join(img_path, 'R10m', 'NDVI.tiff')
        with rio.open(filepath) as src:
            shape_proj = gdf.to_crs(src.crs.data)
            out_image, out_transform = rasterio.mask.mask(
                src, shape_proj.geometry, crop=True)
            out_meta = src.meta.copy()
            out_meta.update(
                {"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})

        filepath = os.path.join(gdf.name.iloc[0], 'NDVI', date+'_ndvi.tiff')
        with rio.open(filepath, 'w', **out_meta) as dst:
            dst.write(out_image)

    def mask_tci(self, img_path, date, poly_name=None):
        '''
        Overview: creates a clipped image of true color file for polygon of interest, saving image in polygon's TCI folder.

        Inputs
            img_path: str
                path to IMG_DATA folder of a product folder in the databse for true color masking. 
            date: datetime
                date corresponding to product folder; date which the images of such folder where taken.
            poly_name: bool
                true if you want to select specific polygon; multipoly case.
        
        Output
            N/A.
        '''
        gdf = self.gdf.iloc[[0]]

        if poly_name != None:
            gdf = self.gdf[self.gdf.name == poly_name]
            
        path = os.path.join(img_path, 'R10m')
        date = date.strftime('%d-%m-%Y')
        bands = {}
        for f in os.listdir(path):
            if '.jp2' in f:
                bands[f.split('_')[2]] = f
        filepath = os.path.join(path, bands['TCI'])
        with rio.open(filepath) as src:
            shape_proj = gdf.to_crs(src.crs.data)
            out_image, out_transform = rasterio.mask.mask(
                src, shape_proj.geometry, crop=True)
            out_meta = src.meta.copy()
            out_meta.update(
                {"driver": "GTiff", "height": out_image.shape[1], "width": out_image.shape[2], "transform": out_transform})

        filepath = os.path.join(gdf.name.iloc[0], 'TCI', date+'_tci.tiff')
        with rio.open(filepath, 'w', **out_meta) as dst:
            dst.write(out_image)
