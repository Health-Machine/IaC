resource "aws_instance" "ec2-health-machine" {
  ami           = "ami-0e86e20dae9224db8"
  instance_type = "t2.micro"

  tags = {
    Name = "ec2-health-machine"
  }

  ebs_block_device {
    device_name = "/dev/sda1"
    volume_size = 30
    volume_type = "gp3"
  }

  security_groups = [aws_security_group.sg_healthmachine_public.id]
  # key_name        = aws_key_pair.learnlink_key_pair.key_name
  key_name = "health-machine-sk"
  subnet_id       = aws_subnet.sn_healthmachine_public.id
}