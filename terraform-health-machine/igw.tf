resource "aws_internet_gateway" "igw_vpc_healthmachine" {
  vpc_id = aws_vpc.vpc_healthmachine.id

  tags = {
    Name = "igw_vpc_healthmachine"
  }
}