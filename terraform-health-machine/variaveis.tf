variable "porta_http" {
    description = "porta http"
    default = 80
    type = number
}

variable "porta_https" {
    description = "porta https"
    default = 443
    type = number
}

variable "porta_ssh" {
    description = "porta ssh"
    default = 22
    type = number
}

variable "zona_disponibilidade" {
    description = "zona_disponibilidade"
    default = "us-east-1"
    type = string
}
