locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "credentials" {
  description = "Service account key file"
  default = "./keys/service-account.json"
}

variable "project" {
  description = "de-stack-overflow"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-central1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bigquery_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "stack_overflow_data"
}