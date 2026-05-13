terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "google_compute_firewall" "miner_axon" {
  name    = "allow-miner-axon"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = [tostring(var.axon_port)]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["synth-miner"]
}

resource "google_compute_instance" "miner" {
  name         = "synth-miner"
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["synth-miner"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.disk_size_gb
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${var.ssh_public_key}"
  }

  metadata_startup_script = templatefile("${path.module}/cloud-init.sh", {
    wallet_name = var.wallet_name
    network     = var.network
    netuid      = var.netuid
    axon_port   = var.axon_port
  })
}

output "miner_ip" {
  value       = google_compute_instance.miner.network_interface[0].access_config[0].nat_ip
  description = "Public IP of the miner VM"
}

output "ssh_command" {
  value       = "ssh ${var.ssh_user}@${google_compute_instance.miner.network_interface[0].access_config[0].nat_ip}"
  description = "SSH command to connect to the miner"
}
