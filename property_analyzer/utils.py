import tarfile
import json
from urllib.request import urlretrieve
from pathlib import Path


def convert_interior_size_to_sqft(building_size_interior: str) -> float:
    size_details = building_size_interior.split(" ")
    size_number = float(size_details[0])
    size_measurement = size_details[1]

    if size_measurement == "sqft":
        return size_number
    elif size_measurement == "m2":
        return size_number * 10.764
    else:
        return 0


city_data = {
    "Toronto": {},
    "Montreal": {"location": [45.5037, -73.6254]},
    "Vancouver": {},
    "Calgary": {},
    "Edmonton": {},
    "Ottawa": {},
}


def download_and_extract_db(
    url="https://github.com/alexmi256/property-analysis/releases/download/v0.0.1/montreal.tar.xz",
):
    file_name = "montreal.tar.xz"
    path, headers = urlretrieve(url, file_name)
    # print(f'Downloaded file {path}:\n{headers}')
    with tarfile.open(file_name) as f:
        f.extractall(filter="data")


def get_metro_lines():
    poi_file = Path("metro-lines.geojson")
    results = []
    if poi_file.exists():
        with open(poi_file) as f:
            metro_lines = json.load(f)["features"]

            for line in metro_lines:
                locations = [list(reversed(x)) for x in line["geometry"]["coordinates"][0]]
                name = line["properties"]["route_name"]
                if "Verte" in name:
                    color = "#008e4f"
                elif "Orange" in name:
                    color = "#ef8122"
                elif "Jaune" in name:
                    color = "#ffe300"
                elif "Bleue" in name:
                    color = "#0083c9"
                else:
                    color = "#FFFFFF"
                results.append({"color": color, "name": name, "locations": locations})
    return results
