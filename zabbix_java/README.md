## Java Zabbix Data Adapter

### Pre-requisites

#### Zabbix
Tested against Zabbix 2.2 with MySQL back end.

#### Prerequisites
- The application is writtin in Java, and requires JDK 1.8 or up.
- maven is required to compile and build the application package.
- please see the pom.xml for details on the dependency library which includes the following:
 - mysql connector 5.1.6
 - wavefront java client 4.26
 - latest version of micrometer
 - slf4j 1.7.25
 - log4j 1.2.17

#### Building the code
Using maven, you can build the code by running the following command

```
mvn clean
mvn package
```

Maven build will produce **wfzabbix.jar** file which you can use to run the adapter.

In order to run the adapter properly, you need two properties files: config.properties and log4j.properties located in the same directory as wfzabbix.jar. If the two files does not exist in the directory where wfzabbix.jar is located, it will try to use the default setting as defined as following:

**config.properties**
```
# zabbix integration configuration file
# wavefront proxy part
wavefront.proxy.host = localhost
wavefront.proxy.port = 2878

# mysql database part
mysql.db.host = localhost
mysql.db.port = 3308
mysql.db.name = zabbix
mysql.db.user = root
mysql.db.password = mysql

# zabbix tables to fetch data from
zabbix.tables = history, history_uint

# prefix which will be added for all metrics
global.prefix = zabbix

# enable sending out JVM metrics
jvm.metrics = true
# source name of the integration
integration.source = zabbix.integration
# whether to enable integration related metrics to be sent to wavefront
integration.metrics = true
# whether to send only profile related metrics
# this mode will NOT send any data, but only calculates number of point data in given interval
integration.profile.only = false
# whether to enable zabbix metrics to be sent to wavefront
integration.wavefront.send = true
# integration batch size - number of metrics contained within a single batch pull
integration.batch.size = 100
# fetch interval (in ms) - main thread will go to sleep for this interval for each fetch and send
# this is set to 5 seconds by default.
integration.fetch.interval = 5000
# fetch incremental
integration.fetch.incremental = 100
# clock delay when db has lagged data (seconds)
integration.fetch.lag = 30
# query timeout in seconds
integration.query.timeout = 10

```

**log4j.properties**
```
# Root logger option
log4j.rootLogger=DEBUG, stdout

# direct log messages to stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.Target=System.out
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1} - %m%n
```
Any settings that you set in your config.properties or log4j.properties will overwrite the above default settings.

The global.prefix allows you to prefix all of the metrics that you retrieve from Zabbix with a common string (set as **zabbix** by default). We recommend leaving this prefix in place so that in future any metrics collected from other sources will be distinguished from Zabbix.

###Running the adapter
To start the adapter, simply run the wfzabbix.jar as java executable.

```
java -jar wfzabbix.jar
```

The adapter requires 1) wavefront proxy, and 2) zabbix server's main MySQL database to be running and accessible. If either of them are not running or reachable, the adapter will fail with exception stack trace.

If everything is running fine, adapter will run with the specified interval (default: 5 seconds), poll Zabbix database to extract any new Zabbix metrics, converts the metrics to Wavefront format, and send it to Wavefront proxy.

In addition to the metrics coming from Zabbix server, there are additional metrics that monitors the zabbix adapter itself, making it convenient for users to monitor its operation using Wavefront.

### Wavefront Dashboard for Zabbix integration
You can import the dashboard into your Wavefront environment to monitor the zabbix adapter. Create a new dashboard, name it as **zabbix-wavefront-integration** as its URL and **Zabbix Wavefront Integration** as its name. Enter the JSON edit mode, by clicking 'EDIT JSON' button at the top right on the Dashboard edit page. Change the view mode from the default **Tree** to **Code** where you can edit the JSON code itself, and paste the **zabbix-wavefront-integration.json** which can be found [here](dashboard/zabbix-wavefront-integration.json) in the git repository.

