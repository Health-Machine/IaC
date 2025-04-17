resource "aws_s3_bucket" "bucket_raw" {
  bucket = "healthmachine-raw-bucket"
  force_destroy = true
}

resource "aws_s3_bucket" "bucket_trusted" {
  bucket = "healthmachine-trusted-bucket"
  force_destroy = true
}

resource "aws_s3_bucket" "bucket_client" {
  bucket = "healthmachine-client-bucket"
  force_destroy = true
}
