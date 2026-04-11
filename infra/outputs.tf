output "vm_ip" {
  description = "IP address of the Prefect VM"
  value       = "192.168.1.113"
}

output "vm_id" {
  description = "Proxmox VM ID"
  value       = proxmox_virtual_environment_vm.prefect.vm_id
}

output "ssh_command" {
  description = "SSH command to connect to the VM"
  value       = "ssh ${var.vm_username}@192.168.1.113"
}
