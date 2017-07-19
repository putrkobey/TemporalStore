output "namespace" {
  value = kubernetes_namespace_v1.temporalstore.metadata[0].name
}

output "proxy_service_name" {
  value = kubernetes_service_v1.proxy.metadata[0].name
}

output "redis_service_name" {
  value = kubernetes_service_v1.redis_proxy.metadata[0].name
}

