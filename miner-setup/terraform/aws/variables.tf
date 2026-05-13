variable "region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-1"
}

variable "instance_type" {
  description = "EC2 instance type (min 4 vCPU, 16GB RAM)"
  type        = string
  default     = "t3.xlarge"
}

variable "disk_size_gb" {
  description = "Root volume size in GB"
  type        = number
  default     = 30
}

variable "key_pair_name" {
  description = "Name of existing EC2 key pair for SSH access"
  type        = string
}

variable "wallet_name" {
  description = "Bittensor wallet name"
  type        = string
  default     = "miner"
}

variable "network" {
  description = "Bittensor network (finney or test)"
  type        = string
  default     = "finney"
}

variable "netuid" {
  description = "Subnet UID"
  type        = number
  default     = 50
}

variable "axon_port" {
  description = "Miner axon port"
  type        = number
  default     = 8091
}
