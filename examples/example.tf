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
  token = "my_token"
}

resource "ydb_table" "table1" {
  # TODO(shmel1k@): do we have ACL now?.
  path              = "table-path"              # Will create table at /path/to/my/table
  database_endpoint = "grpcs://ydb.serverless.cloud-preprod.yandex.net:2135?database=/pre-prod_ydb_public/aoedo0ji1lgce9l91har/cc8pfiaj0ab96vmvp5v8"

  // TODO(shmel1k@): move to YQL
  column {
    name   = "a"
    type   = "Optional<Uint64>" // или по дефолту считать всё Optional и не указывать явно?
//    not_null = true // ADD
  }
  column {
    name   = "b"
    type   = "Optional<Uint8>"
  }
  column {
    name   = "c"
    type   = "Optional<Utf8>"
  }
  column {
    name = "d"
    type = "Optional<Timestamp>"
  }

  primary_key = [
    "a", "b"
  ] // Can not be changed or altered.

  index {
      name    = "index_1_name"
      columns = ["b", "a", "c"]
//      type    = "global" // global_async
  }

  ttl {
    column_name          = "d" # Колонка должна присутствовать в списке колонок.
    mode                 = "date_type" // mode = "since_unix_epoch"
    expire_after_seconds = 10
  }

  attributes = {
    hello = "world"
    privet = "mir"
  }

  auto_partitioning {
    by_size = 2048 # В мегабайтах, если 0, то выключено.
    by_load = true // false -- дефолт.
  }

  partitioning_policy {
    type = "uniform_partitions" // type = "explicit_partitions"
    partitions_count = 42 # Применимо только для uniform_partitions
//    explicit_partitions = [ // Только для type = "explicit_partitions"
//      42, 47, 50 // Границы шардов
//    ]

//    min_partitions_count = 1 # Минимальное количество партиций. Дефолт -- 1.
//    max_partitions_count = 10 # Максимальное количество партиций. Дефолт -- Undefined.
  }

  primary_key_bloom_filter = true # Дефолт -- false

  lifecycle {
    ignore_changes = [
      column, // disables alter
      partitioning_policy, // disables partitioning policy changes
      auto_partitioning
    ]
  }
//
//  ###################################################################################
//  // NOTE(shmel1k@): below are creatable/modifiable attributes, but not really used.
//
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
