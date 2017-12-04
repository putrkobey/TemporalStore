variable "aws_region" {
  description = "AWS region containing the existing EKS cluster."
  type        = string
}

variable "aws_profile" {
  description = "Optional AWS CLI profile to use."
  type        = string
  default     = ""
}

variable "eks_cluster_name" {
  description = "Name of an existing EKS cluster. Terraform reuses this cluster and does not create a new one."
  type        = string
}

variable "namespace" {
  description = "Kubernetes namespace for TemporalStore."
  type        = string
  default     = "temporalstore"
}

variable "image" {
  description = "Container image containing metaserver, server, proxy, and redis_proxy release binaries."
  type        = string
}

variable "image_pull_policy" {
  description = "Kubernetes image pull policy."
  type        = string
  default     = "IfNotPresent"
}

