# You can use the Databricks SDK to manage and retrieve jobs.
# Use Databricks Asset Bundles for source control, local development, and CI/CD. See Python for Databricks Asset Bundles for getting started.

# Install Databricks Assert Bundles package locally
# pip install databricks-bundles==0.248.0
#
# copy contents into resources/nyctaxi_job.py
from databricks.bundles.jobs import Job


nyctaxi_job = Job.from_dict(
    {
        "name": "nyctaxi_job",
        "tasks": [
            {
                "task_key": "ingest_lookup",
                "notebook_task": {
                    "notebook_path": "transformations/notebooks/00_landing/ingest_lookup",
                    "source": "GIT",
                },
            },
            {
                "task_key": "continue_downstream_lookup",
                "depends_on": [
                    {
                        "task_key": "ingest_lookup",
                    },
                ],
                "condition_task": {
                    "op": "EQUAL_TO",
                    "left": "{{tasks.ingest_lookup.values.continue_downstream}}",
                    "right": "yes",
                },
            },
            {
                "task_key": "taxi_zone_lookup",
                "depends_on": [
                    {
                        "task_key": "continue_downstream_lookup",
                        "outcome": "true",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "transformations/notebooks/02_silver/taxi_zone_lookup",
                    "source": "GIT",
                },
            },
            {
                "task_key": "ingest_yellow_trips",
                "notebook_task": {
                    "notebook_path": "transformations/notebooks/00_landing/ingest_yellow_trips",
                    "source": "GIT",
                },
            },
            {
                "task_key": "continue_downstream_yellow_trips",
                "depends_on": [
                    {
                        "task_key": "ingest_yellow_trips",
                    },
                ],
                "condition_task": {
                    "op": "EQUAL_TO",
                    "left": "{{tasks.ingest_yellow_trips.values.continue_downstream}}",
                    "right": "yes",
                },
            },
            {
                "task_key": "yellow_trips_raw",
                "depends_on": [
                    {
                        "task_key": "continue_downstream_yellow_trips",
                        "outcome": "true",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "transformations/notebooks/01_bronze/yellow_trips_raw",
                    "source": "GIT",
                },
            },
            {
                "task_key": "yellow_trips_cleansed",
                "depends_on": [
                    {
                        "task_key": "yellow_trips_raw",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "transformations/notebooks/02_silver/yellow_trips_cleansed",
                    "source": "GIT",
                },
            },
            {
                "task_key": "yellow_trips_enriched",
                "depends_on": [
                    {
                        "task_key": "taxi_zone_lookup",
                    },
                    {
                        "task_key": "yellow_trips_cleansed",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "transformations/notebooks/02_silver/yellow_trips_enriched",
                    "source": "GIT",
                },
            },
            {
                "task_key": "daily_trip_summary",
                "depends_on": [
                    {
                        "task_key": "yellow_trips_enriched",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "transformations/notebooks/03_gold/daily_trip_summary",
                    "source": "GIT",
                },
            },
        ],
        "git_source": {
            "git_url": "https://github.com/bj132175/nyctaxi_project.git",
            "git_provider": "gitHub",
            "git_branch": "main",
        },
        "queue": {
            "enabled": True,
        },
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)