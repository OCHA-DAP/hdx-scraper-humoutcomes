#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.humoutcomes._version import __version__
from hdx.scraper.humoutcomes.pipeline import Pipeline

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-humoutcomes"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: Humoutcomes"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    # User.check_current_user_write_access("")

    with wheretostart_tempdir_batch(folder=_LOOKUP) as info:
        tempdir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=tempdir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=tempdir,
                save=save,
                use_saved=use_saved,
            )
            pipeline = Pipeline(configuration, retriever, tempdir)
            #
            # Steps to generate dataset
            #

            # Get list of countries
            countriesdata = Country.countriesdata()
            countries = []
            for country in countriesdata["countries"].values():
                iso3 = country.get("#country+code+v_iso3")
                countries.append(
                    {
                        "isHRP": Country.get_hrp_status_from_iso3(iso3),
                        "iso2": country.get("#country+code+v_iso2"),
                        "iso3": iso3,
                        "name": country.get("#country+name+preferred"),
                    }
                )

            # Get data for all countries
            pipeline.get_data()

            # Create HRP country datasets
            for country in countries:
                iso3 = country.get("iso3")
                if Country.get_hrp_status_from_iso3(iso3) or iso3 == "PSE":
                    dataset = pipeline.generate_dataset(country)
                    if dataset:
                        dataset.update_from_yaml(
                            script_dir_plus_file(
                                join("config", "hdx_dataset_static.yaml"), main
                            )
                        )
                        dataset["notes"] = dataset["notes"].replace(
                            "(country)", country.get("name")
                        )
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=False,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                            batch=info["batch"],
                        )

            # Create global dataset
            global_dataset = pipeline.generate_global_dataset()
            if global_dataset:
                global_dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                global_dataset["notes"] = (
                    "The Aid Worker Security Database (AWSD) records major incidents of violence against aid workers, with incident reports from 1997 through the present. Initiated in 2005, to date the AWSD remains the sole comprehensive global source of these data, providing the evidence base for analysis of the changing security environment for civilian aid operations."
                )
                global_dataset.create_in_hdx(
                    remove_additional_resources=True,
                    match_resource_order=False,
                    hxl_update=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                    batch=info["batch"],
                )


if __name__ == "__main__":
    facade(
        main,
        hdx_site="demo",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
