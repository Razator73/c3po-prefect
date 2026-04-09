terraform {
  required_providers {
    proxmox = {
      source  = "bpg/proxmox"
      version = "~> 0.73"
    }
  }
}

provider "proxmox" {
  endpoint  = var.proxmox_endpoint
  api_token = var.proxmox_api_token
  insecure  = var.proxmox_insecure

  # Used for disk import and other file operations
  ssh {
    agent            = true
    username         = var.proxmox_ssh_username
    private_key = file(pathexpand(var.proxmox_ssh_private_key_file))
  }
}

resource "proxmox_virtual_environment_vm" "prefect" {
  name      = "c3po-prefect"
  node_name = var.proxmox_node
  vm_id     = 405

  # Graceful shutdown on terraform destroy
  stop_on_destroy = true

  agent {
    # Requires qemu-guest-agent installed in the VM
    enabled = true
  }

  cpu {
    cores = 4
    type  = "x86-64-v2-AES"
  }

  memory {
    dedicated = 8192
  }

  disk {
    datastore_id = "local-zfs"
    # Pre-downloaded manually: wget -O /var/lib/vz/template/iso/noble-server-cloudimg-amd64.img \
    #   https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img
    file_id      = "local:iso/noble-server-cloudimg-amd64.img"
    interface    = "scsi0"
    iothread     = true
    discard      = "on"
    size         = 32
  }

  scsi_hardware = "virtio-scsi-single"

  network_device {
    bridge = "vmbr0"
    model  = "virtio"
  }

  operating_system {
    type = "l26"
  }

  initialization {
    datastore_id = "local-zfs"
    ip_config {
      ipv4 {
        address = "192.168.1.130/24"
        gateway = var.gateway
      }
    }

    user_account {
      username = var.vm_username
      keys     = var.vm_ssh_public_keys
    }
  }
}
