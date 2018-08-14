# culvert
Hive streaming ingest test application

## To run culver
```./culvert -u thrift://localhost:9183 -db test -table culvert -p 64 -n 100000```
The above command will run culvert generated fake data to database 'test' table 'culvert' using 64 threads and each thread commits after every 100K rows.

culvert table schema is
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
