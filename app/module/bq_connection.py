from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)


class BQConnection:
    def __init__(self):
        pass

    def read_bq(self, query, credentials, project_id, job_config):
        client = bigquery.Client(credentials=credentials, project=project_id)
        query_job = client.query(query, job_config=job_config)
        query_job = client.get_job(
            query_job.job_id, location=query_job.location
        )  # Make an API request.
        logger.info(
            "Job {} is currently in state {}".format(query_job.job_id, query_job.state)
        )
        results = query_job.result()
        df = results.to_dataframe()
        return df

    def create_table_feature(self, query, credential, project_id, dataset, table_name):
        client = bigquery.Client(credentials=credential, project=project_id)
        logger.info(
            "Creating temporary table {}.{}.{}".format(project_id, dataset, table_name)
        )

        # Set up job_config
        job_config = bigquery.QueryJobConfig()
        table_ref = client.dataset(dataset).table(table_name)
        job_config.destination = table_ref
        job_config.write_disposition = "WRITE_TRUNCATE"

        # Start the query, passing in the extra configuration.
        query_job = client.query(query, job_config=job_config)  # Make an API request.
        query_job.result()

    def to_bq(self, df, table_id, credentials, project_id, job_config):
        client = bigquery.Client(credentials=credentials, project=project_id)
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

    def dml_bq(self, query, credentials, project_id):
        client = bigquery.Client(credentials=credentials, project=project_id)
        job = client.query(query)
        job.result()
