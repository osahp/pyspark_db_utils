# pyspark_db_utils  

It helps you with your DB deals in Spark

## Documentation

http://pyspark-db-utils.readthedocs.io/en/latest/

## Example of using

You need jdbc drivers for using this lib!
Just get drivers from
https://jdbc.postgresql.org/download.html
https://github.com/yandex/clickhouse-jdbc
and put it in jars/ directory in your project

### Example settings:
```
settings = {
  "PG_PROPERTIES": {
    "user": "user",
    "password": "pass",
    "driver": "org.postgresql.Driver"
  },
  "PG_DRIVER_PATH": "jars/postgresql-42.1.4.jar",
  "PG_URL": "jdbc:postgresql://db.olabs.com/dbname",
}
```

### Example of code

see example.py

### Example of run
```
vsmelov@vsmelov:~/PycharmProjects/pyspark_db_utils$ mkdir jars
vsmelov@vsmelov:~/PycharmProjects/pyspark_db_utils$ cp /var/bigdata/spark-2.2.0-bin-hadoop2.7/jars/postgresql-42.1.4.jar ./jars/
vsmelov@vsmelov:~/PycharmProjects/pyspark_db_utils$ python3 pyspark_db_utils/example.py 
host: ***SECRET***
db: ***SECRET***
user: ***SECRET***
password: ***SECRET***

Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
18/03/05 11:43:29 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/03/05 11:43:29 WARN Utils: Your hostname, vsmelov resolves to a loopback address: 127.0.1.1; using 192.168.43.26 instead (on interface wlp2s0)
18/03/05 11:43:29 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
TRY: create df
OK: create df
+---+-----------+
| id|    mono_id|
+---+-----------+
|  1|          0|
|  2|          1|
|  3|          2|
|  4|          3|
|  5| 8589934592|
|  6| 8589934593|
|  7| 8589934594|
|  8| 8589934595|
|  9| 8589934596|
| 10|17179869184|
| 11|17179869185|
| 12|17179869186|
| 13|17179869187|
| 14|17179869188|
| 15|25769803776|
| 16|25769803777|
| 17|25769803778|
| 18|25769803779|
| 19|25769803780|
+---+-----------+


TRY: write_to_pg
OK: write_to_pg                                                                 

TRY: read_from_pg
OK: read_from_pg
+---+-----------+
| id|    mono_id|
+---+-----------+
| 10|17179869184|
| 11|17179869185|
| 12|17179869186|
| 13|17179869187|
| 14|17179869188|
|  1|          0|
|  2|          1|
|  3|          2|
|  4|          3|
|  5| 8589934592|
|  6| 8589934593|
|  7| 8589934594|
|  8| 8589934595|
|  9| 8589934596|
| 15|25769803776|
| 16|25769803777|
| 17|25769803778|
| 18|25769803779|
| 19|25769803780|
|  1|          0|
+---+-----------+
only showing top 20 rows

```
