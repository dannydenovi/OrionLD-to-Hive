# Big Data Acquisition Project

Requirements:
- OrionLD Docker compose: https://github.com/FIWARE/context.Orion-LD/blob/develop/docker/docker-compose.yml 

## Notes

- OrionLD uses MongoDB as database and it is a requirement since Orion is just at the application level.


## Install HDFS

Install Java 8 with: ```sudo apt-get install openjdk-8-jdk```

Download hadoop: 

```bash
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 ~/hadoop
source ~/.bashrc
```

Then edit ```$HADOOP_HOME/etc/hadoop/core-site.xml``` using: 

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

and also  ```$HADOOP_HOME/etc/hadoop/hdfs-site.xml``` setting the configuration to:

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/$USER/hadoopdata/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/$USER/hadoopdata/hdfs/datanode</value>
  </property>
</configuration>
```

Format the filesystem and run:

```bash
hdfs namenode -format
start-dfs.sh
```

test by launching ```jps```.


If this shows up: ```java.io.IOException: Cannot create directory /home/$USER/hadoopdata/hdfs/namenode/current
```
instead of using ```$USER```use directly the username in the xml files and create the directories manually with 

```bash
mkdir -p /home/${USER}/hadoopdata/hdfs/namenode
mkdir -p /home/${USER}/hadoopdata/hdfs/datanode
```

After doing that enable SSH passwordless on localhost:

```bash
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
```

Remember also to set JAVA_HOME in ```.bashrc```:

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

then 

```bash
echo 'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.ssh/environment
chmod 600 ~/.ssh/environment
```

and enable in ```/etc/ssh/sshd_config``` the option ```PermitUserEnvironment yes``` and restart SSH with ```sudo systemctl restart ssh```.

At the end of the procedure launch ```start-dfs.sh```.


   
## Install HBase

Download HBase:

```bash
wget https://downloads.apache.org/hbase/2.4.18/hbase-2.4.18-bin.tar.gz
tar -xzf hbase-2.4.18-bin.tar.gz
mv hbase-2.4.18 ~/hbase
```

Add variables in `.bashrc`

```bash
export HBASE_HOME=$HOME/hbase
export PATH=$PATH:$HBASE_HOME/bin
```


Configure `~/hbase/conf/hbase-site.xml` with:

```xml
<configuration>
  <property>
	  <name>hbase.rootdir</name>
	  <value>hdfs://localhost:9000/hbase</value>
  </property>
  <property>
	  <name>hbase.cluster.distributed</name>
	  <value>true</value>
  </property>
  <property>
	  <name>hbase.zookeeper.property.dataDir</name>
	  <value>/home/dannydenovi/hbase/zookeeper</value>
  </property>
  <property>
  	<name>hbase.zookeeper.quorum</name>
  		<value>localhost</value>
  	</property>
  <property>
	  <name>hbase.wal.provider</name>
	  <value>filesystem</value>
  </property>
</configuration>
```

Setup the regionservers with:

```bash
echo "localhost" > ~/hbase/conf/regionservers
```

then start HBase with `start-hbase.sh`and look for HMaster and HRegionServer by launching `jps`.


## Docker Version


```bash
docker run -dit \
  --name kylin \
  -p 7070:7070 \
  -p 9090:9090 \
  -p 10000:10000 \
  apachekylin/apache-kylin-standalone:3.1.0 \
  bash
```


inside the container launch:


```bash
/home/admin/hbase-1.1.2/bin/hbase-daemon.sh start thrift -p 9090

```

Then launch:

```bash
hadoop fs -chmod -R 1777 /tmp
hadoop fs -mkdir -p /tmp/hive
hadoop fs -chmod -R 1777 /tmp/hive

hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -chmod -R 777 /user/hive/warehouse

pkill -f hiveserver2 && hive --service hiveserver2 &
```

then connect to HBase using:

```bash
beeline -u jdbc:hive2://localhost:10000
```

- Remember to create the matching external table in Hive with: 

```sql
CREATE EXTERNAL TABLE dim_users_hive (
  rowkey STRING,
  name STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" = ":key,cf:name"
)
TBLPROPERTIES ("hbase.table.name" = "dim_users");
```

but first create the table in HBase or it won't work.


## Python HBase management:

Install the following libraries:

```bash
pip install setuptools thriftpy2 thrift thrift-sasl happybase
```