output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "region" {
  value = var.aws_region
}

output "vpc_id" {
  value = aws_vpc.this.id
}

output "subnet_id" {
  value = aws_subnet.public.id
}

output "efs_id" {
  value = aws_efs_file_system.shared.id
}

output "data_nodes" {
  value = {
    for name, instance in aws_instance.data :
    name => {
      id         = instance.id
      private_ip = instance.private_ip
      public_ip  = instance.public_ip
      cache_ebs  = aws_ebs_volume.data_cache[name].id
