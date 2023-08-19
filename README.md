# Internship-Data Project 2023
**Version 1.0.0**

This project is well structured initiative focused on data scraping, processing and management.

## Contributors

- Dajana Prašo
- Aldin Mešan

## Mentor

- Kemal Pozderac

## Project Structure

- **Scripts Directory:**

This directory contains essential scripts _'scraping_parcels_data.py', 'formatting.py', 'helpers.py'_. They are responsible for parcel data scraping, database management, and data formatting. 
These scripts are crucial components of project's functionality.

- **Input Directory:**

CSV file _'parcel_lat_lng.csv'_ containing latitude and longitude coordinates of specific points is stored in this directory. 
This input data serves as the basis for parcel data processing.

Zip file containing shape file of every county. The shape file has multipolygons that are essential for this script.


- **Database Directory:**

The _'parcel_data.db'_ database is placed in this directory.

- **Output Directory:**

CSV file _'formatted_parcel_data.csv'_ stores the formatted parcel data responses obtained from the database.

- **Centralized Control:**

The main file, _'run.py'_, serves as the central component of this project. All the necessary functions from the "Scripts" directory are imported in this file.

CSV file _'filtered_geom_as_wkt.csv'_ containing geom_as_wkt attribute that represents polygons.
This data is needed for creating multipolygon out of scraped data that is saved in the _'results'_ table of the database.
