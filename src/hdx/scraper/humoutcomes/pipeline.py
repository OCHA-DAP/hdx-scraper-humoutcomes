#!/usr/bin/python
"""Humoutcomes scraper"""

import logging
from typing import Dict, Optional
from urllib.parse import urlencode

import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._data = pd.DataFrame()

    def get_data(self, countries: list):
        """
        Query API once to get data for HRP countries
        """
        iso2_list = [c.get("iso2") for c in countries if c.get("iso2")]
        base_url = f"{self._configuration['base_url']}/search"
        params = {
            "country": ",".join(iso2_list),
            "detail": 1,
            "format": "csv",
        }
        data_url = f"{base_url}?{urlencode(params)}"

        response = self._retriever.download_file(data_url)
        df = pd.read_csv(response, skiprows=[1]).fillna("")
        df["Date"] = pd.to_datetime(
            dict(
                year=pd.to_numeric(df["Year"], errors="coerce"),
                month=pd.to_numeric(df["Month"], errors="coerce"),
                day=pd.to_numeric(df["Day"], errors="coerce"),
            ),
            errors="coerce",
        )

        self._data = df

    def generate_dataset(self, country: Dict) -> Optional[Dataset]:
        """
        Generate country specific dataset
        """
        iso2 = country.get("iso2", "")
        iso3 = country.get("iso3", "")
        country_name = country.get("name", "")

        country_data = self._data[self._data["Country Code"] == iso2].copy()
        if country_data.empty:
            logger.info(f"No data for {country_name}, skipping")
            return

        # Remove lat/lon columns for Palestine
        if iso3 == "PSE":
            country_data = country_data.drop(
                columns=["Latitude", "Longitude"], errors="ignore"
            )

        min_date = pd.to_datetime(country_data["Date"], errors="coerce").min()
        max_date = pd.to_datetime(country_data["Date"], errors="coerce").max()

        # To be generated
        dataset_name = f"aid-worker-security-database-{iso3.lower()}"
        dataset_title = f"{country_name} - {self._configuration['title']}"
        dataset_tags = self._configuration["tags"]

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(min_date, max_date)
        dataset.add_tags(dataset_tags)
        dataset.set_subnational(True)
        dataset.preview_off()
        try:
            dataset.add_country_location(iso3)
        except HDXError:
            logger.error(f"Couldn't find country {iso3}, skipping")
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
            headers=list(country_data.columns),
            iterable=country_data.to_dict(orient="records"),
            hxltags={},
            folder=self._tempdir,
            filename=resource_name,
            resourcedata=resource,
            quickcharts=None,
        )
        return dataset

    def generate_global_dataset(self) -> Optional[Dataset]:
        """
        Generate global dataset
        """
        # Remove data for Palestine
        df = self._data[self._data["Country Code"] != "PS"].copy()

        # Get time period of data
        min_date = pd.to_datetime(df["Date"], errors="coerce").min()
        max_date = pd.to_datetime(df["Date"], errors="coerce").max()

        # Dataset info
        dataset_name = "aid-worker-security-data-global"
        dataset_title = "Global - Aid Worker Security Database"
        dataset_tags = self._configuration["tags"]

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(min_date, max_date)
        dataset.add_tags(dataset_tags)
        dataset.set_subnational(True)
        dataset.preview_off()
        dataset.add_other_location("world")

        # Add resources here
        resource_name = "Global security incidents"
        resource_description = "The AWSD is a global compilation of reports on major security incidents involving deliberate acts of violence affecting aid workers. Annually, the data for the previous year undergoes a verification process to ensure that the data is accurate. For incident descriptions, please download data directly from www.aidworkersecurity.org"
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
