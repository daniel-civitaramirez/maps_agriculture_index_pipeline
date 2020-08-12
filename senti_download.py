import os
import zipfile
import itertools
import numpy as np
import pandas as pd
import geopandas as gpd
import datetime
from sentinelsat import SentinelAPI

class DownloadImages(object):

    '''
        This class is composed by a set of mehtods that download the Sentinel-2 images of
        a given Polygon and updates a diretory with the donnwloaded files.

        Parameters
        ---------

        username: str
            personal username in Copernicus / Sentinel API
        
        password: str
            personal password in Copernicus / Sentinel API
    '''

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.api = SentinelAPI(self.username, self.password,
                               'https://scihub.copernicus.eu/dhus')

    def get_products(self, polygon, initial_date, final_date, cloudcover=(0, 10)):
        '''
        Overview: returns geodataframe of query products from SentinelAPI.

        Inputs
            polygon: Shape
                polygon of interest for api query request.
            inital_date: str
                string in the format 'YMD' of the initial request date. 
            final_date - str 
                string in the format 'YMD' of the final request date.
            cloudcover: tuple, default=(0,10)
                minimum cloud coverage range for api request. 
        
        Output:
            geodataframe of query products.
        
        '''
        products = self.api.query(
            polygon,
            date=(initial_date, final_date),
            platformname='Sentinel-2',
            processinglevel='Level-2A',
            cloudcoverpercentage=cloudcover
        )
        products = self.api.to_geodataframe(products)
        return products

    def download_folders(self, polygon, regional_poly, products, database_path, threshold=0.9):
        '''
        Overview: downloads folders of interest that are not in the current database

        Inputs
            polygon: Shape
                polygon of interest for area intersection calculation.
            regional_poly:
                bool to determine how intersection should be calculated, if region or not. 
            products: geodataframe
                dataframe with all products returned from AI query request.
            database_path: str
                path to the database, where folders are to be downloaded.
            threshold: float, default=0.9
                minimum area of intersection required with polygon to donwload folder.
        
        Output
            returns geodataframe with products that were downloaded.

        '''
        delete = []
        for idx, row in products.iterrows():
            intersect_area = polygon.intersection(row.geometry).area
            if regional_poly:
                area = intersect_area / row.geometry.area
            else:
                area = intersect_area / polygon.area
            if area >= threshold:
                if row.filename not in os.listdir(database_path):
                    #print('downloading: ', row.filename)
                    self.api.download(idx, directory_path=database_path)
                    filepath = os.path.join(database_path, row.title+'.zip')
                    with zipfile.ZipFile(filepath) as zip_ref:
                        zip_ref.extractall(path=database_path)
                else:
                    delete.append(idx)
            else:
                delete.append(idx)
        products = products.drop(delete)
        return products

    def update_downloaded(self,csv_path,downloaded_products):
        '''
        Overview: updates directory with downloaded folders.

        Inputs
            csv_path: str
                path to csv file, filepath.
            downloaded_products: geodataframe
                gdf of all new products that were downloaded.
        
        Output
            N/A.
        '''
        try:
            df = pd.read_csv(csv_path)
            df.append(downloaded_products)
            df.to_csv(csv_path)
        except:
            downloaded_products.to_csv(csv_path)

    def full_pipe(self, csv_path, polygon, intial_date, final_date, database_path, threshold=0.9, cloudcover=(0, 10)):
        '''
        Overview: runs entire Download Images Pipeline, downloading and updating diretory with new products of interest.

        Inputs
            csv_path: str
                path to csv file, filepath.
            polygon: Shape
                polygon of interest for api query request and area threshold calculation to determine which folders to download.
            inital_date: str
                string in the format 'YMD' of the initial request date. 
            final_date - str 
                string in the format 'YMD' of the final request date.
            database_path: str
                path to the database, where folders are to be downloaded.
            threshold: float, default=0.9
                minimum area of intersection required with polygon to donwload folder.
            cloudcover: tuple, default=(0,10)
                minimum cloud coverage range for api request. 
        Output
            N/A.
            
        '''
        products = self.get_products(
            polygon, intial_date, final_date, cloudcover)
        products = self.download_folders(
            polygon, products, database_path, threshold)
        self.update_downloaded(csv_path,products)
