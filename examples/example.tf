terraform {
  required_providers {
    ydb = {}
  }
}

provider "ydb" {
  endpoint = "grpcs://ydb.yandex-team.ru:2135"
  // endpoint = "grpcs://ydb.yandex-team.ru/?database=/Root" also possible
  database = "/Root"
  credentials {
    token = ""
  }
}

resource "ydb_database" "database1" {

}

resource "ydb_table" "table1" {
  path = "/Root/PQ/SourceIdMeta2"
  column {
    name = "a"
    type = "Uint8"
  }
  column {
    name = "b"
    type = "Uint16"
  }
  column {
    name = "c"
    type = "Utf8"
  }
  indexes = [
    {
      name = "index_1_name"
      columns = ["a", "b", "c"]
      type = "global"
    }
  ]
}
