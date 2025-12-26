variable "aws_region" {
  description = "AWS Region"
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project Name"
  default     = "cloud-native-delta"
}

variable "environment" {
  description = "Environment (dev, prod)"
  default     = "dev"
}
