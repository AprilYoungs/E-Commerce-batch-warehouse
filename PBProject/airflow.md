**Python****环境准备**

```sh
# 卸载 mariadb
rpm -qa | grep mariadb 
mariadb-libs-5.5.65-1.el7.x86_64 
mariadb-5.5.65-1.el7.x86_64 
mariadb-devel-5.5.65-1.el7.x86_64

yum remove mariadb
yum remove mariadb-devel
yum remove mariadb-libs

# 安装依赖
rpm -ivh mysql57-community-release-el7-11.noarch.rpm
yum -y install mysql-community-server

yum install readline readline-devel -y
yum install gcc -y
yum install zlib* -y
yum install openssl openssl-devel -y
yum install sqlite-devel -y
yum install python-devel mysql-devel -y

# 提前到python官网下载好包
cd /opt/software
tar -zxvf Python-3.6.6.tgz

# 安装 python3 运行环境
cd Python-3.6.6/
# configure文件是一个可执行的脚本文件。如果配置了--prefix，安装后的所有资源文件都会放在目录中
./configure --prefix=/usr/local/python3.6
make && make install
/usr/local/python3.6/bin/pip3 install virtualenv

# 启动 python3 环境
cd /usr/local/python3.6/bin/
./virtualenv env
. env/bin/activate

# 检查 python 版本
python -V


```

**安装****Airflow**

```sh
# 设置目录(配置文件)
# 添加到配置文件/etc/profile。未设置是缺省值为 ~/airflow
export AIRFLOW_HOME=/opt/servers/airflow

# 使用国内源下载
pip3 install apache-airflow==1.10.11 -i https://pypi.douban.com/simple
```



**创建数据库用户并授权**

```sql
-- 创建数据库
create database airflowcentos72;

-- 创建用户airflow，设置所有ip均可以访问
create user 'airflow'@'%' identified by '12345678';
create user 'airflow'@'localhost' identified by '12345678';

-- 用户授权，为新建的airflow用户授予Airflow库的所有权限
grant all on airflowcentos72.* to 'airflow'@'%';
grant all on *.* to 'airflow'@'localhost';
SET GLOBAL explicit_defaults_for_timestamp = 1; 
flush privileges;
```



```sh
# python3 环境中执行
pip install mysqlclient==1.4.6 
airflow initdb

# 如果遇到下面的错误
# ModuleNotFoundError: No module named 'sqlalchemy.ext.declarative.clsregistry'
pip install "SQLAlchemy==1.3.15"
```

修改 $AIRFLOW_HOME/airflow.cfg:

```sh
 # 约 75 行
sql_alchemy_conn = mysql://airflow:12345678@centos7-2:3306/airflowcentos72

# 重新执行
airflow initdb
```

添加密码文件

python命令，执行一遍;添加用户登录，设置口令

```python
import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

user = PasswordUser(models.User())
user.username = 'airflow'
user.email = 'airflow@lagou.com'
user.password = 'airflow123'

session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()
```

**2.8****、修改时区**

```python
from airflow import configuration
try:
  tz = configuration.conf("core","default_timezone")
  if tz == "system":
    utc = pendulum.local_timezone()
	else:
  	utc = pendulum.timezone(tz)
except Exception:
	pass
```

```sh
# 关闭 airflow webserver 对应的服务
ps -ef | grep 'airflow-webserver' | grep -v 'grep' | awk '{print $2}' | xargs -i kill -9 {}

# 关闭 airflow scheduler 对应的服务
ps -ef | grep 'airflow' | grep 'scheduler' | awk '{print $2}' | xargs -i kill -9 {}

# 删除对应的pid文件
cd $AIRFLOW_HOME
rm -rf *.pid

# 重启服务(在python3.6虚拟环境中执行)
airflow scheduler -D
airflow webserver -D
```

# **元数据管理工具****Atlas****(扩 展)**

添加import hive元数据的时候需要的依赖包

```sh
source_path="/opt/lagou/servers/atlas-1.2.0/server/webapp/atlas/WEB-INF/lib"
target_path="/opt/lagou/servers/atlas-1.2.0/hook/hive/atlas-hive-plugin-impl"
for jar in `ls $source_path`;
	do
		echo $jar
		ln -s $source_path/$jar $target_path/$jar
done
```

```XML
JAR=
jackson-jaxrs-base-2.9.9.jar
jackson-jaxrs-json-provider-2.9.9.jar
jackson-module-jaxb-annotations-2.9.9.jar
jersey-core-1.19.jar

ln -s /opt/lagou/servers/atlas-1.2.0/server/webapp/atlas/WEB-INF/lib/$JAR /opt/lagou/servers/atlas-1.2.0/hook/hive/atlas-hive-plugin-impl/$JAR
```

```sh
 import-hive.sh 
Using Hive configuration directory [/opt/lagou/servers/hive-2.3.7/conf]
Log file for import is /opt/lagou/servers/atlas-1.2.0/logs/import-hive.log
2021-03-21T15:11:22,375 INFO [main] org.apache.atlas.ApplicationProperties - Looking for atlas-application.properties in classpath
2021-03-21T15:11:22,391 INFO [main] org.apache.atlas.ApplicationProperties - Loading atlas-application.properties from file:/opt/lagou/servers/hive-2.3.7/conf/atlas-application.properties
2021-03-21T15:11:22,673 INFO [main] org.apache.atlas.ApplicationProperties - Property (set to default) atlas.graph.cache.db-cache = true
2021-03-21T15:11:22,673 INFO [main] org.apache.atlas.ApplicationProperties - Property (set to default) atlas.graph.cache.db-cache-clean-wait = 20
2021-03-21T15:11:22,673 INFO [main] org.apache.atlas.ApplicationProperties - Property (set to default) atlas.graph.cache.db-cache-size = 0.5
2021-03-21T15:11:22,673 INFO [main] org.apache.atlas.ApplicationProperties - Property (set to default) atlas.graph.cache.tx-cache-size = 15000
2021-03-21T15:11:22,673 INFO [main] org.apache.atlas.ApplicationProperties - Property (set to default) atlas.graph.cache.tx-dirty-size = 120
Enter username for atlas :- admin
Enter password for atlas :- 
Exception in thread "main" java.lang.NoClassDefFoundError: com/sun/jersey/core/util/FeaturesAndProperties
	at java.lang.ClassLoader.defineClass1(Native Method)
	at java.lang.ClassLoader.defineClass(ClassLoader.java:756)
	at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)
	at java.net.URLClassLoader.defineClass(URLClassLoader.java:468)
	at java.net.URLClassLoader.access$100(URLClassLoader.java:74)
	at java.net.URLClassLoader$1.run(URLClassLoader.java:369)
	at java.net.URLClassLoader$1.run(URLClassLoader.java:363)
	at java.security.AccessController.doPrivileged(Native Method)
	at java.net.URLClassLoader.findClass(URLClassLoader.java:362)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:418)
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:355)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:351)
	at org.apache.atlas.hive.bridge.HiveMetaStoreBridge.main(HiveMetaStoreBridge.java:131)
Caused by: java.lang.ClassNotFoundException: com.sun.jersey.core.util.FeaturesAndProperties
	at java.net.URLClassLoader.findClass(URLClassLoader.java:382)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:418)
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:355)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:351)
	... 13 more
Failed to import Hive Meta Data!!!

```

 修改配置文件 $SPARK_HOME/conf/spark-defaults.conf

```sh
spark.master							yarn
spark.eventLog.enabled		true
spark.eventLog.dir 				hdfs://centos7-1:9000/spark/logs
spark.serializer 					org.apache.spark.serializer.KryoSerializer
spark.yarn.jars						hdfs://centos7-1:9000/spark/spark_2.2.1_jars/*
```

修改配置文件spark-env.sh

```sh
export JAVA_HOME=/usr/java/jdk1.8.0_231/
export HADOOP_HOME=/opt/lagou/servers/hadoop-2.9.2/
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

```sh
hdfs dfs -put /opt/lagou/servers/spark-2.2.1/jars/*.jar /spark/spark_2.2.1_jars/
```

```
hello world
hello angie
april you are
my star
you are the angel
oh that's not true
```

```sh
val lines = sc.textFile("/wcinput/wc.txt")
lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect()
```

```conf
elasticsearch hard nofile 1000000
elasticsearch soft nofile 1000000
* soft nproc 4096
* hard nproc 4096
```

运行下面脚本，修改配置

```sh
curl -XPUT http://centos7-2:9200/griffin -d '
{
    "aliases":{

    },
    "mappings":{
        "accuracy":{
            "properties":{
                "name":{
                    "fields":{
                        "keyword":{
                            "ignore_above":256,
                            "type":"keyword"
                        }
                    },
                    "type":"text"
                },
                "tmst":{
                    "type":"date"
                }
            }
        }
    },
    "settings ":{
        "index":{
            "number_of_replicas":"2",
            "number_of_shards":"5"
        }
    }
}
'
```

修改配置文件 service/src/main/resources/application.properties

```sh
server.port = 9876

spring.application.name=griffin_service
spring.datasource.url=jdbc:mysql://centos7-2:3306/quartz?autoReconnect=true&useSSL=false
spring.datasource.username=airflow
spring.datasource.password=12345678
spring.jpa.generate-ddl=true
spring.datasource.driver-class-name=com.mysql.jdbc.Driver
spring.jpa.show-sql=true

# Hive metastore
hive.metastore.uris=thrift://centos7-3:9083
hive.metastore.dbname=hivemetadata
hive.hmshandler.retry.attempts=15
hive.hmshandler.retry.interval=2000ms

# Hive cache time
cache.evict.hive.fixedRate.in.milliseconds=900000

# Kafka schema registry
kafka.schema.registry.url=http://localhost:8081

# Update job instance state at regular intervals
jobInstance.fixedDelay.in.milliseconds=60000

# Expired time of job instance which is 7 days that is 604800000 milliseconds.Time unit only supports milliseconds
jobInstance.expired.milliseconds=604800000

# schedule predicate job every 5 minutes and repeat 12 times at most
#interval time unit s:second m:minute h:hour d:day,only support these four units
predicate.job.interval=5m
predicate.job.repeat.count=12

# external properties directory location
external.config.location=

# external BATCH or STREAMING env
external.env.location=

# login strategy ("default" or "ldap")
login.strategy=default

# ldap
ldap.url=ldap://hostname:port
ldap.email=@example.com
ldap.searchBase=DC=org,DC=example
ldap.searchPattern=(sAMAccountName={0})

# hdfs default name
fs.defaultFS=

# elasticsearch
elasticsearch.host=centos7-2
elasticsearch.port=9200
elasticsearch.scheme=http

# elasticsearch.user = user
# elasticsearch.password = password

# livy
livy.uri=http://localhost:8998/batches
livy.need.queue=false
livy.task.max.concurrent.count=20
livy.task.submit.interval.second=3
livy.task.appId.retry.count=3

# yarn url
yarn.uri=http://centos7-3:8088

# griffin event listener
internal.event.listeners=GriffinJobEventHook
```

![image-20210323102119200](airflow.assets/image-20210323102119200.png)

![image-20210323102515662](airflow.assets/image-20210323102515662.png)

