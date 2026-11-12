variable "region" {
  type    = string
  default = "eu-central-1"
}

variable "snowflake_account" {
  type    = string
  default = "AJPYTLM-HQ40527"
}

variable "alert_email" {
  description = "Where failure alerts go (SNS email subscription; confirm the email AWS sends)."
  type        = string
  default     = ""
}
