variable "organization_name" {
  type    = string
  default = "AJPYTLM"
}

variable "account_name" {
  type    = string
  default = "HQ40527"
}

variable "private_key_path" {
  description = "Key for FINOPS_SVC. Kept outside the repo."
  type        = string
  default     = "~/.snowflake/keys/finops_svc.p8"
}

variable "workload_public_key" {
  description = "Public key (body only) the workload users sign in with."
  type        = string
}
