** Zabbix Data Adapter

*** Zabbix
Tested against Zabbix 2.2 with MySQL back end.

*** Java
- [[zabbix_java/README.md][Java version of Zabbix adapter]] is now available.

*** Python
- The script is written in Python
- Tested on Python 2.7.6 and Python 3.4.0.
- It uses the mysql.connector library. Please ensure this is installed:
  - CentOS :: sudo yum install mysql-connector-python
  - Ubuntu :: sudo apt-get install python-mysql.connector
  - Additional Help :: If the package is not found see: http://dev.mysql.com/doc/connector-python/en/connector-python-installation-binary.html for more details.

**** Configuring the script
At the top of the script there are various configuration parameters. You will need to modify the DB_ ones as appropriate for your Zabbix database. Other options can be left at their defaults if you wish.

The script will pull values from your history and history_uint tables every POLL_INTERVAL seconds. If you modify the LIMIT parameter it will affect the reads from both tables.

We have set the SEND_TO_WF parameter to False initially. This will cause the script to simply print the values it reads to standard out rather than sending anything to Wavefront. Once you have configured the Wavefront Proxy you should change this parameter to True and restart the script.

[[*Wavefront%20Metric%20Format][Metrics]] in Wavefront are strings separated by a . character, E.g.: "system.cpu.load.percpu.avg1". The ZABBIX_PREFIX allows you to prefix all of the metrics that you retrieve from Zabbix with a common string. We recommend leaving this prefix in place so that in future any metrics collected from other sources will be distinguished from Zabbix.

**** Running the script
To get started:

#+BEGIN_EXAMPLE
chmod +x zabbixDBAdapter.py
./zabbixDBAdapter.py
#+END_EXAMPLE

The script will print some output every POLL_INTERVAL. You can stop it at any time by pressing Control-c

The script will save the latest clock interval that it has processed in the files "last_history_clock.hist" and "last_historyuint_clock.hist" which are saved in the same working directory as the script is run from. The initial clock time is "now". If you wish to start retrieving values from some point in the past you can create those files and enter your preferred start time.

When you first run the script with SEND_TO_WF set to False you'll see the values printed to the screen.
