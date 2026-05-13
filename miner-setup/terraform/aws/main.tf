terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_security_group" "miner" {
  name        = "synth-miner"
  description = "Security group for synth miner"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = var.axon_port
    to_port     = var.axon_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "miner" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name               = var.key_pair_name
  vpc_security_group_ids = [aws_security_group.miner.id]

  root_block_device {
    volume_size = var.disk_size_gb
    volume_type = "gp3"
    iops        = 3000
  }

  user_data = templatefile("${path.module}/../gcp/cloud-init.sh", {
    wallet_name = var.wallet_name
    network     = var.network
    netuid      = var.netuid
    axon_port   = var.axon_port
  })

  tags = {
    Name = "synth-miner"
  }
}

output "miner_ip" {
  value       = aws_instance.miner.public_ip
  description = "Public IP of the miner VM"
}

output "ssh_command" {
  value       = "ssh ubuntu@${aws_instance.miner.public_ip}"
  description = "SSH command to connect to the miner"
}
