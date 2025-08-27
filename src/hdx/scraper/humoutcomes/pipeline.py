#!/usr/bin/python
"""Humoutcomes scraper"""

import logging
from typing import Dict, Optional
from urllib.parse import urlencode

import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dateparse import default_date, default_enddate
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._data = pd.DataFrame()

    def get_data(self):
        """
        Query API to get data for all countries
        """
        base_url = f"{self._configuration['base_url']}/search"
        params = {"format": "csv"}
        data_url = f"{base_url}?{urlencode(params)}"
        response = self._retriever.download_file(data_url)
        df = pd.read_csv(response, skiprows=[1], keep_default_na=True)

        # Process date related columns
        for c in ["Year", "Month", "Day"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        # Fill blank values in text columns
        obj_cols = df.select_dtypes(include="object").columns
        df[obj_cols] = df[obj_cols].fillna("")

        self._data = df

    def generate_dataset(self, country: Dict) -> Optional[Dataset]:
        """
        Generate country specific dataset
        """
        iso2 = country.get("iso2", "")
        iso3 = country.get("iso3", "")
        country_name = country.get("name", "")

        country_df = self._data[self._data["Country Code"] == iso2].copy()
        if country_df.empty:
            logger.info(f"No data for {country_name}, skipping")
            return

        # Remove lat/lon columns for Palestine
        if iso3 == "PSE":
            country_df = country_df.drop(
                columns=["Latitude", "Longitude", "City"], errors="ignore"
            )

        # Get date range
        min_date, max_date = self.get_date_range(country_df)

        # Dataset info
        dataset_name = f"aid-worker-security-database-{iso3.lower()}"
        dataset_title = f"{country_name} - {self._configuration['title']}"
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
            headers=list(country_df.columns),
            iterable=country_df.to_dict(orient="records"),
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
        global_df = self._data.copy()

        # Remove sensitive geo data for Palestine
        ps_df = global_df["Country Code"] == "PS"
        cols = ["Latitude", "Longitude", "City"]
        global_df[cols] = global_df[cols].astype("string")
        global_df.loc[ps_df, cols] = ""

        # Get date range
        min_date, max_date = self.get_date_range(global_df)

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
            headers=list(global_df.columns),
            iterable=global_df.to_dict(orient="records"),
            hxltags={},
            folder=self._tempdir,
            filename=resource_name,
            resourcedata=resource,
            quickcharts=None,
        )
        return dataset

    def get_date_range(self, df):
        """
        Returns min and max date
        """
        years = pd.to_numeric(df["Year"], errors="coerce")
        months = pd.to_numeric(df["Month"], errors="coerce").copy()
        days = pd.to_numeric(df["Day"], errors="coerce").copy()

        years = years.fillna(1).astype(int)
        months = months.fillna(1).clip(1, 12).astype(int)
        days = days.fillna(1).clip(1, 31).astype(int)

        # Build datetime Series
        dates = pd.to_datetime(
            {"year": years, "month": months, "day": days},
            errors="coerce",
        )

        if dates.dropna().empty:
            return default_enddate, default_date

        return dates.min(), dates.max()
