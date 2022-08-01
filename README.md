# Eden database

## Motivation

Have a small (up to 100MB at most) database with immutable facts about your project to conveniently check correctness about your project and then possibly generate as much typesafe user code as possible, preventing most of the runtime errors seen today.

- [More about why this is useful](https://mycodingcult.com/index.php/topic,4.0.html)
- [Introduction](https://mycodingcult.com/index.php/topic,27.0.html)

## Compile targets

- ocaml
- rust

"When will ruby/javascript/python be supported?"

![How about no](https://c.tenor.com/8jlC25Qb-jEAAAAC/spiderman-funny.gif)

[Why](https://mycodingcult.com/index.php/topic,24.0.html)

## Syntax example

```
TABLE server {
  hostname TEXT PRIMARY KEY,
  ram_mb INT,
}

TABLE disk_manufacturer {
  model TEXT PRIMARY KEY,
}

TABLE disks {
  disk_id TEXT PRIMARY KEY CHILD OF server,
  size_bytes INT,
  size_mb INT GENERATED AS { size_bytes / 1000000 },
  make REF disk_manufacturer,

  CHECK { size_bytes >= 10000000000 }
}

DATA EXCLUSIVE disk_manufacturer {
  intel;
  crucial;
}

// out data
DATA STRUCT server [
  {
    hostname: my-precious-epyc1, ram_mb: 4096 WITH disks {
        disk_id: root-disk,
        size_bytes: 1000000000000,
        make: intel,
    },
  },
  {
    hostname: my-precious-epyc2, ram_mb: 8192 WITH disks [{
        disk_id: root-disk,
        size_bytes: 1500000000000,
        make: intel,
    },{
        disk_id: data-disk,
        size_bytes: 1200000000000,
        make: crucial,
    }]
  }
]

PROOF "just check that no disks with name 'doofus' exists, works" NONE EXIST OF disks DATALOG {
  OUTPUT(Offender) :- t_disks__disk_id("doofus", Offender).
}

PROOF "no disks exist less than 10 gigabytes" NONE EXIST OF disks {
  SELECT rowid
  FROM disks
  WHERE size_bytes < 10000000000
}
```
