import csv
import json
import logging
import re
import sqlite3
import colorcet as cc


from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path


import folium
from geopy import distance
from shapely import Point, Polygon

from utils import get_metro_lines

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


class MapViewer:
    def __init__(self, db_file: str, city: str = "Montreal", area_of_interest: list[tuple] | None = None):
        """

        :param db_file:
        """
        self.db_file = db_file
        self.city = city
        self.area_of_interest = area_of_interest
        # These should be MLS numbers that I've looked at and never want to see again
        self.blocklist = []
        self.mls_notes = {}
        notes_file = Path("mls_notes.txt")
        if notes_file.exists():
            with open(notes_file) as f:
                lines = f.readlines()
                for line in lines:
                    # lets use, mlsnumber, yes/no, notes
                    data = line.split(",")
                    mls_number = int(data[0].strip())
                    self.mls_notes[mls_number] = {}
                    if len(data) == 1:
                        self.mls_notes[mls_number]["keep"] = False
                    if len(data) > 1:
                        self.mls_notes[mls_number]["keep"] = not (data[1].strip().lower() in ["n", "no", "false", "f"])
                    if len(data) > 2:
                        self.mls_notes[mls_number]["notes"] = data[2].strip()

                self.blocklist.extend([int(x.strip()) for x in f.readlines() if x])

        self.points_of_interest = []
        # https://www.donneesquebec.ca/recherche/dataset/vmtl-stm-traces-des-lignes-de-bus-et-de-metro
        poi_file = Path("stations.geojson")
        if poi_file.exists():
            with open(poi_file) as f:
                poi = json.load(f)
                self.points_of_interest.extend([list(reversed(x["geometry"]["coordinates"])) for x in poi["features"]])

    @staticmethod
    def get_color_for_number_between(number: int | float, minimum: float = 309, maximum: float = 1085) -> str:
        if number < minimum:
            number = minimum
        elif number > maximum:
            number = maximum
        # https://stackoverflow.com/questions/69622670/getting-a-color-range-from-percentage
        ipv = 255 / maximum
        color_number = int(ipv * number)
        # https://colorcet.holoviz.org/user_guide/
        return cc.b_linear_bmy_10_95_c71[color_number]
        # return "#{0:02x}{1:02x}{2:02x}".format(int(ipv * number), 255 - int(ipv * number), 0)

    def get_listings_from_db(
        self,
        min_price: int = 100000,
        max_price: int = 10000000,
        must_have_int_sqft: bool = False,
        must_have_price_change: bool = False,
        no_new_listings: bool = True,
        no_vacant_land: bool = True,
        no_high_rise: bool = True,
        within_area_of_interest: bool = True,
        min_metro_distance_meters: int | None = None,
        min_bedroom: int | None = None,
        min_sqft: int = None,
        max_price_per_sqft: int | None = None,
        last_updated_days_ago: int | None = 7,
        has_garage: bool = False,
        has_parking_details: bool = False,
        has_upcoming_openhouse: bool = False,
        no_undividied: bool = False,
        limit: int = -1,
    ) -> list[dict]:
        with closing(sqlite3.connect(self.db_file)) as connection:
            # This helps maintain the row as a dict
            connection.row_factory = sqlite3.Row
            with closing(connection.cursor()) as cursor:
                conditions = []
                if no_vacant_land:
                    conditions.append(
                        "(Property_ZoningType IS NULL OR Property_ZoningType NOT IN ('Agricultural')) AND Property_Type != 'Vacant Land'"
                    )
                if no_high_rise:
                    conditions.append("Building_StoriesTotal IS NULL OR CAST (Building_StoriesTotal AS INTEGER) < 5")
                if no_new_listings:
                    conditions.append("ComputedNewBuild IS NOT TRUE")
                if must_have_int_sqft:
                    conditions.append("Building_SizeInterior IS NOT NULL")
                if must_have_price_change:
                    # FIXME: Query the DB and find earliest date we have prices for
                    conditions.append(
                        "PriceChangeDateUTC IS NOT NULL AND DATE(PriceChangeDateUTC) > DATE('2023-11-19')"
                    )
                if min_bedroom:
                    conditions.append(f"Building_Bedrooms IS NULL OR Building_Bedrooms >= {min_bedroom}")
                if last_updated_days_ago:
                    conditions.append(f"DATE(ComputedLastUpdated) >= DATE('now', '-{last_updated_days_ago} day')")
                if min_sqft:
                    conditions.append(f"ComputedSQFT IS NULL OR ComputedSQFT >= {min_sqft}")
                if max_price_per_sqft:
                    conditions.append(f"ComputedPricePerSQFT IS NULL OR ComputedPricePerSQFT <= {max_price_per_sqft}")
                if has_garage:
                    conditions.append(f"Property_Parking LIKE '%Garage%'")
                elif has_parking_details:
                    conditions.append(f"Property_Parking IS NOT NULL")
                if no_undividied:
                    conditions.append(f"Property_OwnershipType IS NULL OR Property_OwnershipType != 'Undivided Co-ownership'")

                conditions = [f"({x})" for x in conditions]

                columns_to_select = [
                    "Id",
                    "MlsNumber",
                    "Property_Address_AddressText",
                    "Property_Address_Longitude",
                    "Property_Address_Latitude",
                    "Property_PriceUnformattedValue",
                    "Property_ParkingSpaceTotal",
                    "Property_Parking",
                    "Property_OwnershipType",
                    "Property_Type",
                    "Property_Photo_HighResPath",
                    "Property_AmmenitiesNearBy",
                    "InsertedDateUTC",
                    "PriceChangeDateUTC",
                    "Building_StoriesTotal",
                    "Building_BathroomTotal",
                    "Building_Bedrooms",
                    "Building_Type",
                    "Building_UnitTotal",
                    "Building_SizeInterior",
                    "Building_SizeExterior",
                    "Land_SizeTotal",
                    "Land_SizeFrontage",
                    "AlternateURL_DetailsLink",
                    "RelativeDetailsURL",
                    "AlternateURL_VideoLink",
                    "PostalCode",
                    "PublicRemarks",
                    "ComputedSQFT",
                    "ComputedPricePerSQFT",
                    "ComputedLastUpdated",
                ]

                where_clause = f"""
                    Property_PriceUnformattedValue > {min_price} AND 
                    Property_PriceUnformattedValue < {max_price} AND 
                    {' AND '.join(conditions)}
                """

                if has_upcoming_openhouse:
                    columns_to_select.append("FormattedDateTime")
                    columns_to_select_str = ",\n".join(columns_to_select)
                    query = f"""
                    WITH open_house_unnested AS (
                        SELECT MlsNumber,
                               value AS OpenHouseGeneratedId
                          FROM Listings,
                               json_each(OpenHouse) 
                         WHERE OpenHouse IS NOT NULL
                    ),
                    open_house_in_future AS (
                        SELECT MlsNumber,
                               FormattedDateTime
                          FROM open_house_unnested
                               JOIN
                               OpenHouse USING (
                                   OpenHouseGeneratedId
                               )
                         WHERE DATE(StartDateTime) >= DATE('now') 
                         GROUP BY MlsNumber
                    )
                    SELECT {columns_to_select_str}
                      FROM open_house_in_future
                           JOIN
                           Listings USING (
                               MlsNumber
                           )
                    WHERE 
                       {where_clause}
                    """
                else:
                    columns_to_select_str = ",\n".join(columns_to_select)
                    query = f"""
                        SELECT {columns_to_select_str}
                          FROM Listings
                         WHERE 
                               {where_clause}
                    """

                if limit != -1:
                    query += (f" LIMIT {limit}")

                rows = cursor.execute(query).fetchall()
                listings = [dict(x) for x in rows]
                logging.info(f"Received {len(listings)} listings from the DB")
                # specific_listing = [x for x in listings if x['MlsNumber'] == 26295500]
                # if specific_listing:
                #     pass
                if within_area_of_interest and self.area_of_interest:
                    listings = list(
                        filter(
                            lambda x: Polygon(self.area_of_interest).contains(
                                Point(x["Property_Address_Latitude"], x["Property_Address_Longitude"])
                            ),
                            listings,
                        )
                    )
                    logging.info(f"Filtered down to {len(listings)} listings because of area of interest")

                if min_metro_distance_meters:
                    listings = list(
                        filter(
                            lambda listing: any(
                                [
                                    distance.distance(
                                        [listing["Property_Address_Latitude"], listing["Property_Address_Longitude"]],
                                        poi,
                                    ).meters
                                    < min_metro_distance_meters
                                    for poi in self.points_of_interest
                                ]
                            ),
                            listings,
                        )
                    )
                    logging.info(f"Filtered down to {len(listings)} listings because of points of interest")

                if no_high_rise:
                    listings = list(
                        filter(
                            lambda x: not re.search(r"\|#([5-9]\d{2}|\d{4})\|", x["Property_Address_AddressText"]),
                            listings,
                        )
                    )
                    logging.info(f"Filtered down to {len(listings)} listings because of high apartments")

                return listings

    # FIXME: This function is useless, remove it and just do a regular damn query
    def get_heatmap_data(
        self,
        min_price: int = 100000,
        max_price: int = 5000000,
        within_area_of_interest: bool = True,
        show_per_sqft: bool = False,
    ) -> list[list]:
        """
        There are multiple paramaters when we want to generate a heatmap:
        - do we want it to be for the sample we're intrested in (i.e. price, features) or for a more general population
        - do we want it to be based on listing price or price/sqft
        FIXME: heatmap show lat, long, but I need to convert this in values
        :return: a list containing [lat, long, price]
        """
        heat_data = []
        if show_per_sqft:
            listings = self.get_listings_from_db(
                min_price=min_price,
                max_price=max_price,
                must_have_int_sqft=True,
                within_area_of_interest=within_area_of_interest,
            )
            for listing in listings:
                weight = listing["ComputedPricePerSQFT"]
                heat_data.append(
                    [
                        float(listing["Property_Address_Latitude"]),
                        float(listing["Property_Address_Longitude"]),
                        weight,
                        listing["AlternateURL_DetailsLink"],
                        listing["RelativeDetailsURL"],
                    ]
                )
        else:
            listings = self.get_listings_from_db(
                min_price=min_price,
                max_price=max_price,
                must_have_int_sqft=False,
                within_area_of_interest=within_area_of_interest,
            )
            for listing in listings:
                weight = listing["Property_PriceUnformattedValue"]
                heat_data.append(
                    [
                        float(listing["Property_Address_Latitude"]),
                        float(listing["Property_Address_Longitude"]),
                        weight,
                        listing["AlternateURL_DetailsLink"],
                        listing["RelativeDetailsURL"],
                    ]
                )

        return heat_data

    def export_data_to_csv(self, listings):
        listings_to_save = []
        cols_for_csv = [
            "MlsNumber",
            "Notes",
            "Property_PriceUnformattedValue",
            "ComputedPricePerSQFT",
            "Property_Address_AddressText",
            "Propery_CostPerSQFT",
            "Property_ParkingSpaceTotal",
            "Property_Parking",
            "Property_OwnershipType",
            "Property_Type",
            "Property_Photo_HighResPath",
            "Property_AmmenitiesNearBy",
            "ComputedSQFT",
            "InsertedDateUTC",
            "PriceChangeDateUTC",
            "Building_StoriesTotal",
            "Building_BathroomTotal",
            "Building_Bedrooms",
            "Building_Type",
            "Building_UnitTotal",
            "Building_SizeInterior",
            "Building_SizeExterior",
            "Land_SizeTotal",
            "Land_SizeFrontage",
            "AlternateURL_DetailsLink",
            "RelativeDetailsURL",
            "AlternateURL_VideoLink",
            "PublicRemarks",
            "ComputedLastUpdated",
        ]
        for listing in listings:
            has_custom_notes = listing["MlsNumber"] in self.mls_notes
            custom_notes = self.mls_notes[listing["MlsNumber"]].get("notes") if has_custom_notes else ""
            if custom_notes:
                listing["Notes"] = custom_notes
                if self.mls_notes.get(listing["MlsNumber"], {}).get("keep") is False:
                    continue
            listings_to_save.append(listing)
        with open("listings_to_audit.csv", "w") as csv_file:
            dict_writer = csv.DictWriter(csv_file, cols_for_csv, extrasaction="ignore")
            dict_writer.writeheader()
            dict_writer.writerows(listings_to_save)

    def display_price_changes(self):
        my_map = folium.Map(location=(45.5037, -73.6254), tiles=None, zoom_start=14)

        # Layers
        folium.TileLayer("OpenStreetMap").add_to(my_map)

        if self.area_of_interest:
            folium.Polygon(self.area_of_interest, tooltip="Area of Interest").add_to(my_map)

        for metro_line in get_metro_lines():
            folium.PolyLine(
                metro_line["locations"], color=metro_line["color"], tooltip=metro_line["name"], weight=8
            ).add_to(my_map)

        listings_with_price_changes = self.get_listings_from_db(
            min_price=200000,
            max_price=5000000,
            no_high_rise=False,
            no_new_listings=False,
            within_area_of_interest=False,
            must_have_price_change=True,
            last_updated_days_ago=90,
        )

        for listing_with_price_changes in listings_with_price_changes:
            with closing(sqlite3.connect(self.db_file)) as connection:
                with closing(connection.cursor()) as cursor:
                    # FIXME: This is slow and terrible, this should be done in one query
                    sql = f"""
                    SELECT Price, Date
                      FROM PriceHistory
                     WHERE MlsNumber = {listing_with_price_changes['MlsNumber']}
                     ORDER BY DATE(Date) ASC
                     LIMIT 1;
                    """
                    oldest_price = cursor.execute(sql).fetchone()[0]
            current_price = listing_with_price_changes["Property_PriceUnformattedValue"]
            # IDK WTF
            if oldest_price == current_price:
                continue
            change_amount = (float(current_price) / oldest_price - 1) * 100
            if change_amount == 0 or change_amount > 200:
                # Just a rounding error I don't care about or price mistakes
                continue
            elif change_amount > 0:
                color = "green"
            else:
                color = "red"

            change_amount = abs(int(change_amount))
            popup = f"Current {current_price}, Previous {oldest_price} ({change_amount})"

            folium.Circle(
                [
                    listing_with_price_changes["Property_Address_Latitude"],
                    listing_with_price_changes["Property_Address_Longitude"],
                ],
                radius=20 * change_amount,
                color=color,
                fill=True,
                fill_opacity=0.1,
                fill_color=color,
                weight=1,
                popup=popup,
            ).add_to(my_map)

        folium.LayerControl().add_to(my_map)

        my_map.save(f"{self.city}_price_changes.html")

    def add_heat_data_to_map(self, folium_map: folium.Map, is_per_sqft=False):
        listings = self.get_listings_from_db(
            min_price=200000,
            max_price=2000000,
            must_have_int_sqft=is_per_sqft,
            within_area_of_interest=False,
            no_high_rise=True,
            min_bedroom=2,
            last_updated_days_ago=90,
        )

        keyname_for_price = "Property_PriceUnformattedValue" if is_per_sqft is False else "ComputedSQFT"

        price_range = [x[keyname_for_price] for x in listings]
        if is_per_sqft:
            min_range = 100
            max_range = 1000
        else:
            min_range = 400000
            max_range = 1000000

        print(f"Heat map range is between {min_range} and {max_range}")

        for listing in listings:
            popup_html = f"""
                        <img src="{listing['Property_Photo_HighResPath']}" width="320">
                        <b>${listing['Property_PriceUnformattedValue']}</b> ${listing['ComputedPricePerSQFT']}/sqft {listing['MlsNumber']} <br>
                        {listing['Property_Address_AddressText']} <br>
                        {listing['Building_Bedrooms']}BDR, {listing['Building_BathroomTotal']}BA, {listing['ComputedSQFT']}sqft, {listing['Building_Type']} <br>
                        <a href="{listing['AlternateURL_DetailsLink']}" target="_blank">Details</a> <a href="https://www.realtor.ca{listing['RelativeDetailsURL']}" target="_blank">MLS</a> <br>
                        Last Seen: {listing['ComputedLastUpdated']} <br>
                        Parking: {listing['Property_Parking']}, {listing['Property_AmmenitiesNearBy']} <br>
                        {listing.get('FormattedDateTime', '')}
                        """

            circle_color = MapViewer.get_color_for_number_between(
                listing[keyname_for_price], minimum=min_range, maximum=max_range
            )

            folium.Circle(
                location=[listing["Property_Address_Latitude"], listing["Property_Address_Longitude"]],
                radius=70,
                color=circle_color,
                fill=True,
                fill_opacity=0.5,
                fill_color=circle_color,
                weight=1,
                popup=popup_html,
                tooltip=listing[keyname_for_price],
            ).add_to(folium_map)

    def display_listings_on_map(self, listings, display_heatmap=True):
        my_map = folium.Map(location=(45.5037, -73.6254), tiles=None, zoom_start=14)

        # Layers
        folium.TileLayer("OpenStreetMap").add_to(my_map)

        if self.area_of_interest:
            folium.Polygon(self.area_of_interest, tooltip="Area of Interest").add_to(my_map)

        for metro_line in get_metro_lines():
            folium.PolyLine(
                metro_line["locations"], color=metro_line["color"], tooltip=metro_line["name"], weight=8
            ).add_to(my_map)

        for poi in self.points_of_interest:
            folium.Circle(
                poi, radius=20, color="black", fill=True, fill_opacity=1.0, fill_color="white", weight=1
            ).add_to(my_map)

        if display_heatmap:
            # Plugin heatmap is trash and based on density not weights
            self.add_heat_data_to_map(my_map, is_per_sqft=False)

        for listing in listings:
            icon_color = "blue"
            marker_color = "white"
            has_custom_notes = listing["MlsNumber"] in self.mls_notes
            custom_notes = self.mls_notes[listing["MlsNumber"]].get("notes", '').lower() if has_custom_notes else ""
            internet_status = "📠" if custom_notes and "bad_internet" in custom_notes.lower() else ""
            last_updated = (
                "👴"
                if datetime.strptime(listing["ComputedLastUpdated"][:10], "%Y-%m-%d") > datetime.now() + timedelta(days=-7)
                else "👶"
            )

            if not listing["Property_Parking"]:
                garage_status = "❌🅿️"
            elif "Garage" in listing["Property_Parking"]:
                garage_status = "🅿️"
            else:
                garage_status = "🤔🅿️"

            if listing["PriceChangeDateUTC"]:
                price_history = " 🗠" + listing["PriceChangeDateUTC"][:10]
                # TODO: Once we have a history db, try to look it up
            else:
                price_history = ""

            if listing["ComputedPricePerSQFT"]:
                icon_color = MapViewer.get_color_for_number_between(listing["ComputedPricePerSQFT"])

            tooltip = f"${listing['Property_PriceUnformattedValue']}, {listing['Building_Bedrooms']}BDR ${listing['ComputedPricePerSQFT']}/sqft, {garage_status}{internet_status}{last_updated} {price_history} {custom_notes}"

            popup_html = f"""
            <img src="{listing['Property_Photo_HighResPath']}" width="320">
            <b>${listing['Property_PriceUnformattedValue']}</b> ${listing['ComputedPricePerSQFT']}/sqft {listing['MlsNumber']}{price_history}<br>
            {listing['Property_Address_AddressText']} <br>
            {listing['Building_Bedrooms']}BDR, {listing['Building_BathroomTotal']}BA, {listing['ComputedSQFT']}sqft, {listing['Building_Type']} <br>
            <a href="{listing['AlternateURL_DetailsLink']}" target="_blank">Details</a> <a href="https://www.realtor.ca{listing['RelativeDetailsURL']}" target="_blank">MLS</a> <br>
            Last Seen: {listing['ComputedLastUpdated']} <br>
            Ownership: {listing.get('Property_OwnershipType')} <br>
            Parking: {listing['Property_Parking']}, {listing['Property_AmmenitiesNearBy']} <br>
            {custom_notes}
            {listing.get('FormattedDateTime', '')}
            """
            # folium.Popup("Let's try quotes", parse_html=True, max_width=100)

            if custom_notes:
                if (
                    any(x in custom_notes for x in ["condo fee", "bad internet", 'hard no', 'basement', 'sold'])
                    or (
                        datetime.strptime(listing["ComputedLastUpdated"][:10], "%Y-%m-%d")
                        < datetime.now() + timedelta(days=-7)
                    )
                ):
                    marker_color = "black"
                    house_icon = "circle-xmark"
                elif self.mls_notes[listing["MlsNumber"]].get("keep") is False:
                    house_icon = "circle-xmark"
                    marker_color = "lightgray"
                elif any(x in custom_notes for x in ["sam"]):
                    marker_color = "pink"
                    icon_color = ("#ff0000",)
                    house_icon = "question-circle"
                elif any(x in custom_notes for x in ["contacted", "saw"]):
                    marker_color = "orange"
                    house_icon = "circle-check"
                else:
                    house_icon = "circle-check"
                    marker_color = "lightblue"

            elif listing["Building_Type"] == "House":
                house_icon = "house"
            elif listing["Building_Type"] in ["Apartment"]:
                house_icon = "building"
            else:
                house_icon = "city"

            folium.Marker(
                location=[listing["Property_Address_Latitude"], listing["Property_Address_Longitude"]],
                tooltip=tooltip,
                popup=popup_html,
                icon=folium.Icon(icon=house_icon, prefix="fa", color=marker_color, icon_color=icon_color),
            ).add_to(my_map)

        folium.LayerControl().add_to(my_map)

        my_map.save(f"{self.city}.html")


aoi = [
    (45.546780742201165, -73.65807533729821),
    (45.5241750187359, -73.67472649086267),
    (45.51022227302072, -73.69086266029626),
    (45.50156020795671, -73.67524147499353),
    (45.48796289057615, -73.65258217323571),
    (45.467741340888665, -73.61258507240564),
    (45.45690538269222, -73.59181404579431),
    (45.454256276138466, -73.563661579974),
    (45.46990828260759, -73.55662346351892),
    (45.48038065986003, -73.54512215126306),
    (45.50601171342892, -73.5449504898861),
    (45.53241273092978, -73.54306221473962),
    (45.56006337665252, -73.6131000565365),
    (45.547682377783296, -73.63163948524743),
    (45.54972603156036, -73.65429878700525),
]


# Enable this if you want to download the data from GitHub
# download_and_extract_db()

db_file = "montreal_full.sqlite"
if not Path(db_file).exists():
    raise Exception("Can't run code if the DB does not exist")


viewer = MapViewer(db_file, city="Montreal", area_of_interest=aoi)

relevant_listings = viewer.get_listings_from_db(
    min_price=350000,
    max_price=690000,
    within_area_of_interest=False,
    min_metro_distance_meters=2000,
    min_bedroom=2,
    min_sqft=920,
    max_price_per_sqft=660,
    # has_upcoming_openhouse=True,
    # no_undividied=True,
    # has_parking_details=True,
    # has_garage=True,
)
# viewer.export_data_to_csv(relevant_listings)
viewer.display_listings_on_map(relevant_listings, display_heatmap=False)


# viewer.display_price_changes()