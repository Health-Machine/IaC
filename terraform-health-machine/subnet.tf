resource "aws_subnet" "sn_healthmachine_public" {
  vpc_id     = aws_vpc.vpc_healthmachine.id
  cidr_block = "10.0.1.0/24"
  availability_zone = "us-east-1a"
  tags = {
    Name = "sn_healthmachine_public"
  }

    // determina se as instâncias executadas na VPC recebem nomes de host DNS públicos que correspondem a seus endereços IPpúblicos.
  enable_resource_name_dns_a_record_on_launch = true

  // Determina que uma EC2 criada nessa sub-rede recebe automaticamente um endereço IPv4 público
  map_public_ip_on_launch = true
}
