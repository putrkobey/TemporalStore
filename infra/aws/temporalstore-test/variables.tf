variable "aws_profile" {
  description = "Local AWS CLI profile to use for deployment."
  type        = string
  default     = "temporalstore"
}

variable "aws_region" {
  description = "AWS region for TemporalStore resources."
  type        = string
  default     = "us-west-2"
}

variable "name_prefix" {
  description = "Prefix for named AWS resources."
  type        = string
  default     = "temporalstore-test"
}

variable "availability_zone" {
  description = "Single AZ for the first small functional test."
  type        = string
  default     = "us-west-2a"
