

## Example 1
### run producer in collab
### run batch in dataproc

gcloud dataproc batches submit pyspark gs://edit-data-eng-dev/datalake/scripts/vehicles_stream.py --batch vehiclesstream --deps-bucket=edit-data-eng-dev --region=europe-west1

## Example 2
### run batch in dataproc

gcloud dataproc batches submit pyspark gs://edit-data-eng-dev/datalake/scripts/rate_stream.py --batch ratestream --deps-bucket=edit-data-eng-dev --region=europe-west1

### create dataproc cluster to analyze data