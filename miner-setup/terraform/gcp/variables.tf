variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "europe-west1-b"
}

variable "machine_type" {
  description = "GCE machine type (min 4 vCPU, 16GB RAM)"
  type        = string
  default     = "e2-standard-4"
}

variable "disk_size_gb" {
  description = "Boot disk size in GB"
  type        = number
  default     = 30
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

variable "ssh_public_key" {
  description = "SSH public key for VM access"
  type        = string
}

variable "ssh_user" {
  description = "SSH username"
  type        = string
  default     = "miner"
}
