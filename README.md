## Repository for course: database II
This readme helps you to perform the intended labs in classroom regarding topics such as mapreduce with hadoop, hive, spark and kafka.

### docker related  
Build the image, create an internal network and run the image using a local volumen
path to share files and jars from the host computer
```
docker build . -t hadoop

docker network create --driver bridge --subnet 10.0.0.0/28 littlenet

docker run -it -p 9000:9000 -p 9092:9092 -p 22:22 -v /Users/desarrollador/Projects/hadoopbases2/mapr:/home/hadoopuser/mapr --name hadoopserver --net littlenet --ip 10.0.0.2 hadoop
```

This is an example of how to manually copy files from the host to the container 
```
docker cp maprexample.jar hadoopserver:/home/hadoopuser
docker cp datadates.csv  hadoopserver:/home/hadoopuser
```
Get result from hive
```
docker cp  datasets/ hadoopserver:/home/hadoopuser/datasets
mv result/000000_0 result/hive.csv
docker cp hadoopserver:/home/hadoopuser/result/hive.csv pyspark/examples/hive.csv
```

### ssh related
The image includes a default user setup, the user "hadoopuser" must grant passwordless access by ssh, this is required for the hadoop server

```
su - hadoopuser
cd /home/hadoopuser
ssh-keygen -t rsa -P '' -f /home/hadoopuser/.ssh/id_rsa
ssh-copy-id hadoopuser@localhost
exit
```

### hadoop related
These are the commands to start/stop the hadoop single node cluster 
```
start-all.sh
stop-all.sh
```

These are example of instructions to prepare hdfs folders and run a map reduce example
Directories for Hive input
```
hadoop fs -mkdir /data
hadoop fs -mkdir /data/input
hadoop fs -mkdir /data/input/soccer
hadoop fs -copyFromLocal datasets/soccer/ /data/input/

hadoop fs -copyFromLocal datasales.dat /data/input
hadoop jar maprexample.jar main.program /data/input/datadates.csv /data/output
```

### hive related
To setup the hive environment just run the `hive-setup.sh` script located in hadoopuser home folder

Then access the hive console with `hive`

The following is an example of instructions in hive console to test your hive environment. The example loads the content of the CSV file datasales.dat into a temporary table where all the fields are string. Following the transfer of the data to the correct table using data types. 

Once data is loaded, run some queries to test the performance 
```
SELECT MONTH(fecha), YEAR(fecha), SUM(monto) from sales group by YEAR(fecha), MONTH(fecha);

SELECT anyo, MAX(monto) from (
    SELECT MONTH(fecha) mes, YEAR(fecha) anyo, SUM(monto) monto from sales group by YEAR(fecha), MONTH(fecha)
) as tabla 
group by anyo;
```

### Kakfa related
To start the kafkta server just the script `start-kafka.sh` located in the hadoopuser home folder.

To test your Kafka environment follow the [kafka quickstart guide](https://kafka.apache.org/quickstart)