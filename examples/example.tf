terraform {
  required_providers {
    ydb = {
       source = "terraform.storage.ydb.tech/provider/ydb"
    }
  }
}

# To be discussed.
# Do we really any configuration parameters except token?
provider "ydb" {
//  credentials {
//    token = ""
//  }
}

// resource "ydb_database" "database1" {
//   // TODO(shmel1k@): add after control-plane!?
// }

resource "ydb_table" "table1" {
  # TODO(shmel1k@): do we have ACL now?.
//  acl {
//  }
  path              = "table-path"              # Will create table at /path/to/my/table
  database_endpoint = "grpc://mr-nvme-testing.search.yandex.net:8761/?database=/local"
  column {
    name   = "a"
    type   = "Uint8"
    family = "smth"
  }
  column {
    name   = "b"
    type   = "Uint16"
    family = "smth"
  }
  column {
    name   = "c"
    type   = "Utf8"
    family = "smth"
  }
  primary_key = [
    "a", "b"
  ] // Can not be changed or altered(???).

  index {
      name    = "index_1_name"
      columns = ["a", "c", "b"]
//      type    = "global"
  }

  ttl {
    column_name          = "d"
    mode                 = "date_type" // mode = "since_unix_epoch"
    expire_after_seconds = 10
  }
//
//  ###################################################################################
//  // NOTE(shmel1k@): below are creatable/modifiable attributes, but not really used.
//
//  attribute {
//    key   = "privet"
//    value = "mir"
//  }
//  attribute {
//    key   = "hello"
//    value = "world"
//  }
//
//  profile {
//    storage_policy {
//      syslog {
//        # storage_pool
//        media = ""
//      }
//      log = {
//        # storage_pool
//      }
//      data = {
//        # storage_pool
//      }
//      external = {
//        # storage_pool
//      }
//      keep_in_memory = true # if defined
//      column_families = [
//        {
//          name = "privet"
//          data = {
//            # storage_pool
//          }
//          external = {
//            # storage_pool
//          }
//          compression = "compressed" # todo: add enum
//        }
//      ]
//    }
//    compaction_policy {
//      preset_name = "preset_name" # TODO: add preset names
//    }
//    partitioning_policy {
//      preset_name = "" # TODO: preset names?
//      auto_partitioning = "unspecified" # TODO: add enum
//      partitions = {
//        uniform_partitions = {
//          uniform_partitions = 42
//        }
//
//        # XXX(shmel1k@): mutually exclusive with uniform_partitions
//        explicit_partitions = {
//          split_point {
//            value = "42" # golang interface if possible
//          }
//          split_point {
//            value = "privet"
//          }
//        }
//      }
//    }
//    execution_policy {
//      preset_name = ""
//    }
//    replication_policy {
//      preset_name = ""
//      replicas_count = 2
//      create_per_availability_zone = true # false | undef
//      allow_promotion = true # false | undef
//    }
//    caching_policy {
//    }
//  }
}

# Старое описание
// resource "ydb_stream" "stream1" {
//   name                = "streams/my/stream-path" # Will create stream at /streams/my directory.
//   retention_period_ms = 1000 * 60 * 60 * 24 * 1  # длительность хранения данных в стриме
//   partitions_count    = 2                        # количество партиций
//   supported_codecs = [                           # поддерживаемые кодеки
//     "raw",
//   ]
// }

// resource "ydb_queue" "queue1" {
//   # YMQ should be enabled in any ydb database, just like streams.
//   name = "sqs/my/queue/path" # Will create queue at "sqs/my/queue/path" // SQS path?..
//   # TODO(shmel1k@): FirstClassCitizen queues?
//   database_endpoint = "grpcs://..."
// 
//   visibility_timeout_seconds  = var.visibility_timeout_seconds
//   message_retention_seconds   = var.message_retention_seconds
//   max_message_size            = var.max_message_size
//   delay_seconds               = var.delay_seconds
//   receive_wait_time_seconds   = var.receive_wait_time_seconds
//   policy                      = var.policy
//   redrive_policy              = var.redrive_policy
//   redrive_allow_policy        = var.redrive_allow_policy
//   fifo_queue                  = var.fifo_queue
//   content_based_deduplication = var.content_based_deduplication
//   deduplication_scope         = var.deduplication_scope
//   fifo_throughput_limit       = var.fifo_throughput_limit
// 
//   tags = var.tags
// }
