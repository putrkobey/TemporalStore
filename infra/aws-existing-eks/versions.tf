terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.38"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile == "" ? null : var.aws_profile
}

data "aws_eks_cluster" "this" {
  name = var.eks_cluster_name
}

