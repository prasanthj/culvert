# culvert
Hive streaming ingest test application

## Running culvert
Before running culvert, checkout the project and build it
```
git clone https://github.com/prasanthj/culvert.git
cd culvert
mvn clean install
```

Use culvert cli tool
```
./culvert --help
Example usage: culvert -n 100000 -t 60000 -e 100

usage: Culvert
 -b,--transaction-batch-size <arg>     size of transaction batch. default
                                       = 1
 -d,--enable-dynamic-partition         enable dynamic  partitioned insert
                                       (destination table has to be
                                       partitioned correctly). default =
                                       false
 -db <arg>                             destination database. default =
                                       default
 -e,--events-per-second <arg>          events/records per second (values
                                       >1000 will all be same, as
                                       1000/events-per-second will be used
                                       as sleep interval). default =
                                       10_0000
 -f,--disable-auto-flush               disable auto-flush of open orc
                                       files. default = false
 -h,--help                             usage help
 -l,--stream-launch-delay <arg>        delay in milliseconds between
                                       launching streams. default = 0
 -n,--commit-after-n-rows <arg>        commit transaction  after every n
                                       rows. default = 1_000_000
 -p,--parallelism <arg>                number of parallel streams. default
                                       = 1
 -s,--disable-streaming-optimization   disables all streaming
                                       optimizations. default = false
 -t,--timeout <arg>                    timeout in milliseconds after which
                                       all streams in culvert will be
                                       stopped. default = 60000
 -table <arg>                          destination table. default =
                                       culvert
 -u,--metastore-url <arg>              remote metastore url. default =
                                       'thrift://localhost:9083'
```

## Table Schema
```
create table if not exists culvert (
user_id string,
page_id string,
ad_id string,
ad_type string,
event_type string,
event_time string,
ip_address string)
partitioned by (year int, month int)
clustered by (user_id)
into 32 buckets
stored as orc
tblproperties("transactional"="true");
```

## Sample Run

*NOTE:* Before running the following command, make sure metastore service is running and serving at 9083 port, database and table with above schema already exists. The table and metastore should meet the requirements from https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest+V2#StreamingDataIngestV2-StreamingRequirements

```./culvert -u thrift://localhost:9083 -db test -table culvert -p 64 -n 100000```

The above command will run culvert generated fake data to database 'test' table 'culvert' using 64 threads and each thread commits after every 100K rows.
