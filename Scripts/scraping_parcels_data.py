import json
import pandas as pd
import requests
import time
from Scripts.helpers import insert_data_into_db, create_buffer_points
from shapely.wkt import loads
from shapely import Polygon, MultiPolygon
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point


def fetch_polygon_vertices(lat, lng):
    """
        Fetches polygon vertices from a web API based on the given latitude and longitude.

        This function sends a request to a web API to retrieve parcel data for a specific location,
        extracts the polygon vertices from the response, and returns them as a list.

        :param lat: Latitude of the location.
        :param lng: Longitude of the location.
        :return: A list of (lat, lng) coordinates representing the vertices of the fetched polygon,
                 or None if no data is found.
        """
    try:
        headers = {
            'X-Auth-Token': 'LYswcDvqGD6boAJNseAc',
            'X-Auth-Email': 'damn-joy@gmail.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188',
        }
        url = f'https://green.parcels.id.land/parcels/parcels/by_location.json?lng={lng}&lat={lat}'
        response = requests.get(url, headers=headers)
        response_json = response.json()

        polygon_vertices = None

        if 'parcels' in response_json:
            parcels = response_json['parcels']

            if parcels:
                for parcel_info in parcels:
                    parcel_data = parcel_info['parcel_data']
                    parcel_polygon_wkt = parcel_data['geom_as_wkt']

                    parcel_polygon = loads(parcel_polygon_wkt)
                    polygon_vertices = list(parcel_polygon.exterior.coords)
            else:
                print("No parcels found in the response.")
        else:
            print("No 'parcels' key found in the response.")

        return polygon_vertices

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API request: {e}")

    except KeyError:
        print("Error: Failed to extract data from the response.")
        return None


def scrape_data(lat, lng):
    """
    Scrape parcel data using the given latitude and longitude.

    :param lat: Latitude of the location.
    :param lng: Longitude of the location.
    :return: JSON response containing scraped parcel data.
    """
    headers = {
        'X-Auth-Token': 'LYswcDvqGD6boAJNseAc',
        'X-Auth-Email': 'damn-joy@gmail.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188',
    }
    url = f'https://green.parcels.id.land/parcels/parcels/by_location.json?lng={lng}&lat={lat}'
    response = requests.get(url, headers=headers)

    try:
        response_json = response.json()
    except ValueError:
        response_json = {}

    return response_json.get("parcels", [])


def filter_function(point_list, multipolygon):
    filtered_points = []

    for lat, lng in point_list:
        point = Point(lng, lat)
        if point.within(multipolygon):
            filtered_points.append((lat, lng))

    return filtered_points


def scrape_and_save_data(connection, cursor):
    """
    Scrapes data for latitude and longitude points from the 'input' table in the database,
    modifies the response format, and saves the scraped data to a new CSV file.

    :param connection: The SQLite connection to the database.
    :param cursor: The SQLite cursor to execute SQL queries.
    :return: None
    """
    BUFFER_DISTANCES = [0.010, 0.00020]
    # Load the multipolygon boundary from the shapefile
    boundary_shapefile = Path(
        'C:\\Users\\Datasoft\\Desktop\\novo\\internship-data-project-2023\\Input\\COUNTY-2020-US-SL050-Coast-Clipped'
        '.zip')
    # Specify the COUNTYFP code of the specific county you want to extract
    target_county_geo_fips = '48381'  # Randall County, Texas
    multipolygon = MultiPolygon([gpd.read_file(boundary_shapefile)[gpd.read_file(boundary_shapefile)['Geo_FIPS'] ==
                                                                   target_county_geo_fips]['geometry'].to_crs
                                 (epsg=4326).values[0],
                                 gpd.read_file(boundary_shapefile)[gpd.read_file(boundary_shapefile)['Geo_FIPS'] ==
                                                                   target_county_geo_fips]['geometry'].to_crs
                                 (epsg=4326).values[0]])

    buffer_distances = BUFFER_DISTANCES

    max_iterations = 3  # Maximum number of iterations
    iteration = 0

    while iteration < max_iterations:
        cursor.execute("SELECT id, lat, lng, status FROM input WHERE status = 'TODO'")
        data = cursor.fetchall()

        if not data:
            break

        df = pd.DataFrame(data, columns=['id', 'lat', 'lng', 'status'])
        df['response'] = None

        buffer_points_array = []

        for _, row in df.iterrows():
            connection.execute("UPDATE input SET status = 'PROCESSING' WHERE id = ?", (row['id'],))
            connection.commit()

            lat = row['lat']
            lng = row['lng']
            # Check if the point is inside the boundary
            point = Point(lng, lat)
            polygon_vertices = fetch_polygon_vertices(lat, lng)
            is_inside_boundary = point.within(multipolygon)

            if not is_inside_boundary:
                print(f"Point ({lat}, {lng}) is outside the boundary. Skipping.")
                continue

            if polygon_vertices:
                polygon = Polygon(polygon_vertices)
                buffer_zones = [polygon.buffer(distance) for distance in buffer_distances]

                # Scrape data from original point and buffer points
                for buffer_zone in buffer_zones[:2]:
                    buffer_points_df = create_buffer_points(buffer_zone, buffer_distances, num_points=2)

                    if buffer_points_df is not None and not buffer_points_df.empty:
                        # Filter the buffer points using the filter_function
                        filtered_buffer_points = filter_function(buffer_points_df[['lat', 'lng']].values, multipolygon)

                        if filtered_buffer_points:
                            buffer_points_df = buffer_points_df[
                                buffer_points_df[['lat', 'lng']].apply(tuple, axis=1).isin(filtered_buffer_points)]
                            if not buffer_points_df.empty:
                                buffer_points_df['id'] = row['id']
                                buffer_points_array.append(buffer_points_df)

                                # Scrape data for each combined point
                                for _, combined_row in buffer_points_df.iterrows():
                                    combined_lat = combined_row['lat']
                                    combined_lng = combined_row['lng']
                                    scraped_data = scrape_data(combined_lat, combined_lng)

                                    if not scraped_data:
                                        insert_data_into_db(json.dumps(None), cursor, combined_row['id'])
                                    else:
                                        insert_data_into_db(json.dumps(scraped_data[0]), cursor, combined_row['id'])

                            connection.execute(
                                "UPDATE input SET status = 'DONE' WHERE id = ?",
                                (row['id'],))
                            connection.commit()
                    else:
                        # No buffer points found, mark as DONE without response
                        connection.execute(
                            "UPDATE input SET status = 'DONE' WHERE id = ?",
                            (row['id'],))
                        connection.commit()

            connection.execute(
                "UPDATE input SET status = 'DONE' WHERE id = ?",
                (row['id'],))
            connection.commit()

            time.sleep(0.1)

        # Insert buffer points into the input table
        for buffer_points_df in buffer_points_array:
            for _, combined_row in buffer_points_df.iterrows():
                combined_lat = combined_row['lat']
                combined_lng = combined_row['lng']
                connection.execute("INSERT INTO input (lat, lng, status) VALUES (?, ?, ?)",
                                   (combined_lat, combined_lng, 'TODO'))
            connection.commit()

        iteration += 1

        # Check if it's the last iteration and process remaining 'TODO' points
        if iteration == max_iterations:
            while True:
                cursor.execute("SELECT id, lat, lng, status FROM input WHERE status = 'TODO'")
                remaining_data = cursor.fetchall()

                if not remaining_data:
                    break

                for remaining_row in remaining_data:
                    remaining_id, remaining_lat, remaining_lng, _ = remaining_row
                    connection.execute("UPDATE input SET status = 'PROCESSING' WHERE id = ?", (remaining_id,))
                    connection.commit()

                    polygon_vertices = fetch_polygon_vertices(remaining_lat, remaining_lng)

                    if polygon_vertices:
                        polygon = Polygon(polygon_vertices)
                        buffer_zones = [polygon.buffer(distance) for distance in buffer_distances]

                        # Check if the remaining point is within the boundary
                        polygon_within_boundary = polygon.within(multipolygon)

                        if polygon_within_boundary:
                            # Scrape data from original point and buffer points
                            for buffer_zone in buffer_zones[:2]:
                                buffer_points_df = create_buffer_points(buffer_zone, buffer_distances, num_points=4)

                                if buffer_points_df is not None and not buffer_points_df.empty:
                                    # Filter the buffer points using the filter_function
                                    filtered_buffer_points = filter_function(buffer_points_df[['lat', 'lng']].values,
                                                                             multipolygon)

                                    if filtered_buffer_points:
                                        buffer_points_df = buffer_points_df[
                                            buffer_points_df[['lat', 'lng']].apply(tuple, axis=1).isin(
                                                filtered_buffer_points)]
                                        if not buffer_points_df.empty:
                                            buffer_points_df['id'] = remaining_id
                                            buffer_points_array.append(buffer_points_df)

                                            # Scrape data for each combined point
                                            for _, combined_row in buffer_points_df.iterrows():
                                                combined_lat = combined_row['lat']
                                                combined_lng = combined_row['lng']
                                                scraped_data = scrape_data(combined_lat, combined_lng)

                                                if not scraped_data:
                                                    insert_data_into_db(json.dumps(None), cursor, combined_row['id'])
                                                else:
                                                    insert_data_into_db(json.dumps(scraped_data[0]), cursor,
                                                                        combined_row['id'])

                                            connection.execute(
                                                "UPDATE input SET status = 'DONE' WHERE id = ? AND status = "
                                                "'PROCESSING'",
                                                (remaining_id,))
                                            connection.commit()

                    connection.execute(
                        "UPDATE input SET status = 'DONE' WHERE id = ? AND status = 'PROCESSING'",
                        (remaining_id,))
                    connection.commit()

    print("Scraping and processing completed.")
