import json
import pandas as pd


def parse_json_data(json_str):
    """
    Parses JSON data from a JSON-encoded string.

    :param json_str: JSON-encoded string to be parsed.
    :return: Parsed data as a Python list or dictionary.
    """
    try:
        if json_str.strip() is None:
            return None
        parsed_data = json.loads(json_str)
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def process_and_save_filtered_data(output_csv_filename, cursor_param):
    """
    Filters the data from the 'results' table, replaces empty lists with none, and saves it to a new CSV file.
    :param cursor_param: The SQLite database cursor object to execute the insertion query.
    :param output_csv_filename: The name of the output CSV file to save the filtered data.
    :return: None
    """
    cursor_param.execute("SELECT id, origin_id, data FROM results")
    data_rows = cursor_param.fetchall()

    all_data_frames = []
    all_field_keys = set()

    for row in data_rows:
        row_id, origin_id, json_data = row
        if json_data is None:
            continue
        else:

            if isinstance(json_data, str):
                json_data = parse_json_data(json_data)
            if json_data is None:
                continue

            parcel_data = {}

            for _ in json_data:
                parcel_data.update(json_data.get('parcel_data', {}))
                field_data = json_data.get('field_data', [])

                for entry in field_data:
                    name = entry.get('name')
                    value = entry.get('value')
                    if name is not None and value is not None:
                        name = name.lower().replace(' ', '_')
                        all_field_keys.add(name)
                        parcel_data[f'field_data_{name}'] = value

            parcel_data['id'] = row_id
            parcel_data['origin_id'] = origin_id

            all_data_frames.append(parcel_data)

        df = pd.DataFrame(all_data_frames)

        field_data_cols = [col for col in df.columns if col not in ['id', 'origin_id']]

        all_keys = ['id', 'origin_id'] + field_data_cols
        df = df.reindex(columns=all_keys)
        df.to_csv(output_csv_filename, index=False)

        selected_cols2 = ['geom_as_wkt']
        df = df[selected_cols2]

        df.to_csv('filtered_geom_as_wkt.csv', index=False)
