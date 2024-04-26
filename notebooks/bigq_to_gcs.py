from google.cloud import bigquery

project = "bigquery-public-data"

bigq_tables = ['posts_questions', 'posts_answers', 'badges', 'users']
bigq_dataset_id = "stackoverflow"
gcs_bucket_name = 'dtc_data_lake_de-stack-overflow'

client = bigquery.Client()


for table_id in bigq_tables:

    gcs_destination_uri = "gs://{}/{}".format(gcs_bucket_name, f"{table_id}-*.csv")
    
    bigq_dataset_ref = bigquery.DatasetReference(project, bigq_dataset_id)
    bigq_table_ref = bigq_dataset_ref.table(table_id)

    extract_job = client.extract_table(
        bigq_table_ref,
        gcs_destination_uri,
        # Location must match that of the source table.
        location="US",
    )  # API request
    
    extract_job.result()  # Waits for job to complete.

    print("Exported {}:{}.{} to {}".format(project, bigq_dataset_id, table_id, gcs_destination_uri)