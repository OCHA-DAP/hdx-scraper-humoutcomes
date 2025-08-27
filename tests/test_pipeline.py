from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.humoutcomes.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestHumoutcomes",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)

                countries = [{"iso2": "CO", "iso3": "COL", "name": "Colombia"}]
                pipeline.get_data()

                dataset = pipeline.generate_dataset(countries[0])
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )

                assert dataset == {
                    "caveats": None,
                    "name": "aid-worker-security-database-col",
                    "title": "Colombia - Aid Worker Security Database",
                    "dataset_date": "[1997-10-18T00:00:00 TO 2025-05-26T23:59:59]",
                    "dataset_preview": "no_preview",
                    "tags": [
                        {
                            "name": "aid worker security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "aid workers",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by",
                    "methodology": "This data was compiled using media reports, incident reports, and by a process of annual verification in which affected and non-affected agencies are contacted to provide data.",
                    "dataset_source": "Humanitarian Outcomes",
                    "groups": [{"name": "col"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "1",
                    "maintainer": "fdbb8e79-f020-4039-ab3a-9adb482273b8",
                    "owner_org": "9675c871-7b87-4f08-86f8-fd53f7809096",
                    "data_update_frequency": 1,
                    "notes": "This dataset shows aid worker security incidents in (country). "
                    "Annually, the data for the previous year undergoes a verification "
                    "process. Data for the current year is provisional. For incident "
                    "descriptions, please download data directly from "
                    "[www.aidworkersecurity.org](www.aidworkersecurity.org)\n",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "AWSD_CO_security_incidents.csv",
                        "description": "This dataset shows aid worker security incidents in Colombia.",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                for resource in resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(fixtures_dir, filename)
                    assert_files_same(actual, expected)
