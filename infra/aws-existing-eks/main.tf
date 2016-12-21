locals {
  labels = merge(
    {
      "app.kubernetes.io/name"       = "temporalstore"
      "app.kubernetes.io/managed-by" = "terraform"
    },
    var.labels,
  )

  meta_service_name   = "temporalstore-metaserver"
  server_service_name = "temporalstore-server"
  proxy_service_name  = "temporalstore-proxy"
  redis_service_name  = "temporalstore-redis"

  meta_addr             = "${local.meta_service_name}.${var.namespace}.svc.cluster.local:17001"
  server_advertise_addr = "${local.server_service_name}.${var.namespace}.svc.cluster.local:17002"
  proxy_addr            = "${local.proxy_service_name}.${var.namespace}.svc.cluster.local:17000"

  validation_jobs = {
    raft = {
      name    = "temporalstore-raft-validation"
      command = ["/usr/local/bin/distributed_raft_harness"]
      args    = var.raft_validation_args
    }
    scale = {
      name    = "temporalstore-scale-validation"
      command = ["/usr/local/bin/scale_harness"]
      args    = var.scale_validation_args
    }
    storage = {
      name    = "temporalstore-storage-validation"
      command = ["/usr/local/bin/storage_modes_harness"]
      args    = var.storage_validation_args
    }
  }
}

resource "kubernetes_namespace_v1" "temporalstore" {
  metadata {
    name   = var.namespace
    labels = local.labels
  }
}

resource "kubernetes_persistent_volume_claim_v1" "server_data" {
  metadata {
    name      = "temporalstore-server-data"
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = local.labels
  }

  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class_name == "" ? null : var.storage_class_name
    resources {
      requests = {
        storage = var.server_storage_size
      }
    }
  }
}

resource "kubernetes_deployment_v1" "metaserver" {
  metadata {
    name      = local.meta_service_name
    namespace = kubernetes_namespace_v1.temporalstore.metadata[0].name
    labels    = merge(local.labels, { "app.kubernetes.io/component" = "metaserver" })
  }

  spec {
    replicas = 1
    selector {
      match_labels = { app = local.meta_service_name }
    }
    template {
      metadata {
        labels = merge(local.labels, { app = local.meta_service_name, "app.kubernetes.io/component" = "metaserver" })
      }
      spec {
        container {
          name              = "metaserver"
          image             = var.image
          image_pull_policy = var.image_pull_policy
          command           = ["/usr/local/bin/metaserver"]
          env {
            name  = "TS_META_BIND_ADDR"
            value = "0.0.0.0:17001"
          }
          port {
            name           = "http"
            container_port = 17001
          }
          readiness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
          }
          liveness_probe {
            http_get {
              path = "/health"
              port = "http"
            }
          }
        }
      }
