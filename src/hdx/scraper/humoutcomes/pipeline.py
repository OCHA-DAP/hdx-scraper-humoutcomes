#!/usr/bin/python
"""Humoutcomes scraper"""

import logging
from typing import Optional

import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def get_data(self):
        data_url = f"{self._configuration['base_url']}/search?detail=1&format=csv"
        response = self._retriever.download_file(data_url)
        df = pd.read_csv(response, skiprows=[1]).fillna("")

        # Drop rows with no location information
        df = df.dropna(subset=["Country Code"])
        return df
        # for code, data in df.groupby("Country Code"):
        #     self.generate_dataset(data)

    def generate_dataset(self, df: pd.DataFrame) -> Optional[Dataset]:
        country_name = df.loc[0, "Country"]
        iso2 = df.loc[0, "Country Code"]
        iso3 = Country.get_iso3_from_iso2(iso2)

        if not iso3:
            logger.info(f"No country code for {iso2}")
            return

        # Remove lat/lon columns for Palestine
        if iso2 == "PS":
            df = df.drop(columns=["Latitude", "Longitude"])

        df["Date"] = pd.to_datetime(
            dict(year=df["Year"], month=df["Month"], day=df["Day"]), errors="coerce"
        )
        min_date = df["Date"].min()
        max_date = df["Date"].max()

        # To be generated
        dataset_name = f"aid-worker-security-database-{iso3.lower()}"
        dataset_title = f"{country_name} - {self._configuration['title']}"
        dataset_tags = self._configuration["tags"]
        dataset_country_iso3 = iso3

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(min_date, max_date)
        dataset.add_tags(dataset_tags)
        # Only if needed
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
            return

        # Add resources here
        resource_name = f"AWSD_{iso2}_security_incidents.csv"
        resource_description = (
            f"This dataset shows aid worker security incidents in {country_name}."
        )
        resource = {
            "name": resource_name,
            "description": resource_description,
        }

        dataset.generate_resource_from_iterable(
            headers=list(df.columns),
            iterable=df.to_dict(orient="records"),
            hxltags={},
            folder=self._tempdir,
            filename=resource_name,
            resourcedata=resource,
            quickcharts=None,
        )

        return dataset
