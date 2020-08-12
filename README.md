## Local Proof of Concept Solution: preprocesses files into geopandas and geojsons, downloads Sentinel-2 images for such processed areas of interest (storing locally with local csv database), generates NDVI and NDRE indicies, clips and saves cut area of interest. 

Mask_Pipeline: Solution Demonstration.
Local_sol_poc: Diagram of POC Pipeline. 

Preprocess_file: preprocessing shp, kml, and geojsons files, accounting for multipoly polygons.
Senti_download: download Sentinel-2 images for geopanda.
Img_preprocessing: generate NDRI and NDRE and clips area of interest of geopandas.
