## Repository for second project database II
This project intends to use two data sets in order to predict the value of a given player.

### docker related  
Build the image, create an internal network and run the image using a local volumen
path to share files and jars from the host computer
```
docker build . -t hadoop

docker network create --driver bridge --subnet 10.0.0.0/28 littlenet

docker run -it -p 9000:9000 -p 9092:9092 -p 22:22 -v /Users/desarrollador/Projects/hadoopbases2/mapr:/home/hadoopuser/mapr --name hadoopserver --net littlenet --ip 10.0.0.2 hadoop
//docker run -it -p 9000:9000 -p 9092:9092 -p 22:22 -v "C:\Users\Mau\Documents\TEC\2020\Semestre II\Bases II\Progra II\JugadoresRevelacion\mapr":/home/hadoopuser/mapr --name hadoopserver --net littlenet --ip 10.0.0.2 hadoop
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
```
### MapR related
```
cd mapr
Instructions to prepare hdfs folders and run a map reduce
hadoop fs -copyFromLocal European_Rosters.csv /data/input
hadoop jar market.jar main.program /data/input/European_Rosters.csv /data/output
hadoop fs -get /data/output/part-r-00000
mv part-r-00000 mapr.csv
```


Commands to access results
```
hdfs dfs -rm -r /data/output/
hadoop fs -cat /data/output/part-r-00000
cat ../../../opt/hadoop/hadoop-3.3.0/logs/userlogs/application_1612109796205_0006/container_1612109796205_0006_01_000003/stdout
../../../../../../../../home/hadoopuser/mapr
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