variable "proxmox_endpoint" {
  description = "Proxmox API URL, e.g. https://192.168.1.x:8006/"
  type        = string
}

variable "proxmox_api_token" {
  description = "Proxmox API token in format: user@realm!token_id=token_secret"
  type        = string
  sensitive   = true
}

variable "proxmox_insecure" {
  description = "Skip TLS verification (needed if Proxmox uses a self-signed cert)"
  type        = bool
  default     = true
}

variable "proxmox_node" {
  description = "Proxmox node name (e.g. 'deathstar' or 'coruscant')"
  type        = string
  default     = "deathstar"
}

variable "proxmox_ssh_username" {
  description = "SSH username for Proxmox host (used for disk imports)"
  type        = string
  default     = "root"
}

variable "proxmox_ssh_private_key_file" {
  description = "Path to SSH private key for authenticating to Proxmox host"
  type        = string
  default     = "~/.ssh/b-wing_rsa"
}

variable "gateway" {
  description = "Default gateway for the VM"
  type        = string
  default     = "192.168.1.1"
}

variable "vm_username" {
  description = "Username to create inside the VM"
  type        = string
  default     = "ubuntu"
}

variable "vm_ssh_public_keys" {
  description = "SSH public keys to authorize for the VM user (one per line or as a list)"
  type        = list(string)
}
