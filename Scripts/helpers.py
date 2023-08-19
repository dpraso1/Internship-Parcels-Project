import json
from datetime import datetime, timezone, timedelta
import pandas as pd
from shapely import MultiPolygon, Polygon, Point
from shapely.wkt import loads as wkt_loads

buffer_distances = [0.00010, 0.00020, 0.01000]


def create_multipolygon_from_csv(csv_filename):
    """

    :param csv_filename: The file path of the CSV file containing the data to be inserted.
    :return:
    """
    input_data_df = pd.read_csv(csv_filename)
    polygon_list = []
    for index, row in input_data_df.iterrows():
        wkt_geometry = row['geom_as_wkt'].strip('"')  # Remove double quotes from the WKT string
        polygon = wkt_loads(wkt_geometry)
        polygon_list.append(polygon)
    return MultiPolygon(polygon_list)


def find_buffer_points_on_corners(geometry, distance, num_points=4):
    """
    Finds points along the boundary corners of a buffer polygon created around the input polygon.

    :param geometry: The input geometry (either a Polygon or MultiPolygon) to create a buffer around.
    :param distance: The distance used to create the buffer polygon.
    :param num_points: The number of points to be generated along the boundary corners. Default is 4.
    :return: A list of (x, y) coordinates representing points along the boundary corners.
    """
    buffer_points = []

    if isinstance(geometry, MultiPolygon):
        geometries = geometry.geoms
    elif isinstance(geometry, Polygon):
        geometries = [geometry]
    else:
        raise ValueError("Invalid geometry type")

    for geom in geometries:
        buffer_polygon = geom.buffer(distance)
        for i in range(num_points):
            point_on_boundary = buffer_polygon.boundary.interpolate(i / num_points, normalized=True)
            buffer_point = (point_on_boundary.x, point_on_boundary.y)

            if not is_point_within_multipolygon_wkt(buffer_point):
                buffer_points.append(buffer_point)

    return buffer_points


def is_point_within_multipolygon_wkt(point):
    """
    Checks if a given point is within a multipolygon based on its well-known text (WKT) representation.

    :param point: The point that needs to be checked. Should be in the format (longitude, latitude).
    :return: True if the point is within the multipolygon, False otherwise.
    """
    filename = r'C:\Users\Datasoft\Desktop\novo\internship-data-project-2023\filtered_geom_as_wkt.csv'
    multipolygon = create_multipolygon_from_csv(filename)

    point_geom = Point(point)
    for geometry in multipolygon.geoms:
        if isinstance(geometry, MultiPolygon):
            for polygon in geometry.geoms:
                if polygon.contains(point_geom):
                    return True
        elif isinstance(geometry, Polygon):
            if geometry.contains(point_geom):
                return True
    return False


def create_buffer_points(polygon, buffers_distances, num_points=4):
    """
    Generates points along the boundary corners of buffer polygons around the given polygon.

    This function iterates through a series of buffer distances, creates buffer polygons
    around the input polygon, and finds points along the boundary corners of those buffer polygons.

    :param polygon: The input polygon for which buffer points are to be generated.
    :param buffers_distances: A list of buffer distances used to create buffer polygons.
    :param num_points: The number of points to be generated along the boundary corners. Default is 2.
    :return: A DataFrame containing points along the boundary corners of buffer polygons,
             with columns 'lng' (longitude) and 'lat' (latitude), or None if no valid points are found.
    """

    for buffer_distance in buffers_distances[:2]:  # Iterate through the specified number of buffer zones
        buffer_points = find_buffer_points_on_corners(polygon, buffer_distance, num_points)
        print("Buffer Points:", buffer_points)
        if buffer_points:
            buffer_df = pd.DataFrame(buffer_points, columns=['lng', 'lat'])
            return buffer_df

    # If no valid response is found in the first two buffer zones, proceed to the third buffer zone
    if len(buffer_distances) >= 3:
        buffer_distance = buffer_distances[2]
        buffer_points = find_buffer_points_on_corners(polygon, buffer_distance, num_points)
        print("Buffer Points2:", buffer_points)
        if buffer_points:
            buffer_df = pd.DataFrame(buffer_points, columns=['lng', 'lat'])
            return buffer_df

    return None


def create_logs_and_results_tables(cursor):
    """
    Create the 'logs' and 'results' tables in the SQLite database if they don't exist.

    :param cursor: The cursor object to execute SQL queries.
    :return: None
    """

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            log TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_id INTEGER,
            data TEXT,
            FOREIGN KEY (origin_id) REFERENCES input (id)
        )
    ''')


def insert_log(cursor):
    """
    Inserts a log entry with the current timestamp into the 'logs' table.

    :param cursor: The SQLite cursor to execute the query.
    :return: None
    """
    utc_now = datetime.utcnow()
    local_timezone = timezone(timedelta(hours=4))
    local_time = utc_now.astimezone(local_timezone)
    timestamp_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("INSERT INTO logs (log, timestamp) VALUES (?, ?)", ("", timestamp_str))
    cursor.connection.commit()


def insert_data_into_db(response_str, cursor, origin_id):
    """
    Inserts data into the SQLite database table 'results' and logs the insertion.
    If a duplicate 'geom_as_wkt' is found, skips the insertion.

    :param response_str: The JSON response data in string format to be inserted.
    :param cursor: The SQLite cursor to execute SQL queries.
    :param origin_id: The parameter that represents an identifier
    associated with the data being inserted into the database.
    :return: None
    """
    if response_str is not None:
        try:
            geom_as_wkt = json.loads(response_str)['parcel_data']['geom_as_wkt']
            # Check if a record with the same 'geom_as_wkt' already exists
            cursor.execute("SELECT id FROM results WHERE json_extract(data, '$.parcel_data.geom_as_wkt') = ?",
                           (geom_as_wkt,))
            existing_record = cursor.fetchone()
            if not existing_record:
                # Insert a new record
                cursor.execute("INSERT INTO results (origin_id, data) VALUES (?, ?)", (origin_id, response_str))
                print("Inserted new record.")
            else:
                print("Skipped insertion due to duplicate geom_as_wkt.")
            insert_log(cursor)
        except (TypeError, KeyError):
            print("Error: Failed to extract data from the response.")

# The original function
# def insert_data_into_db(response_str, cursor, origin_id):
#     """
#     Inserts data into the SQLite database table 'results' and logs the insertion.
#
#     param response_str: The JSON response data in string format to be inserted.
#     param cursor: The SQLite cursor to execute SQL queries.
#     param origin_id: The parameter that represents an identifier
#     associated with the data being inserted into the database.
#     :return: None
#     """
#
#     cursor.execute("INSERT INTO results (origin_id, data) VALUES (?, ?)", (origin_id, response_str))
#
#     insert_log(cursor)
