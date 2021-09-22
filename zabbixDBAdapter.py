#!/usr/bin/python
"""Script to poll metrics from MySQL Database and push them to Wavefront."""

from __future__ import print_function

import os
import sys
import re
import time
import datetime
import socket
import signal
import itertools
import mysql.connector

from wavefront_sdk.client_factory import WavefrontClientFactory

# USER CONFIGURATION ###

# All metrics will be prefixed with this text. Please ensure it has a trailing
ZABBIX_PREFIX = "zabbix."

# Frequency at which records will be retrieved from the DB in seconds
POLL_INTERVAL = 60

# Limit the amount of records that will be pulled from the DB on each POLL_INTERVAL
LIMIT = 10000

# Set to False to print values rather than sending to Wavefront
SEND_TO_WF = False

WAVEFRONT_PROXY_HOST = "localhost"
WAVEFRONT_PROXY_PORT = 2878

DB_SERVER = "localhost"
DB_DATABASE = "zabbix"
DB_UNAME = "root"
DB_PW = "password"

# Store the latest Epoch time that has already been sent to Wavefront in
# these files. There is a separate file for each Zabbix telemetry table -
# history and history_uint The "history" table stores metrics with
# float/double values and the "history_uint" table stores metrics
# with Integer values.
HISTORY_CLOCK_FILEPATH = "last_history_clock.hist"
HISTORYUINT_CLOCK_FILEPATH = "last_historyuint_clock.hist"

# END USER CONFIGURATION ###

# SQLs. The results of SQL_QUERY_ITEMIDS are used within SQL_QUERY_HISTORY
# to force MySQL to use the history_1 index
# which is created by default by the Zabbix install.
SQL_QUERY_ITEMIDS = "select distinct itemid from items"

HISTORY_TABLE = "history"
HISTORY_TABLE_UINT = "history_uint"

SQL_QUERY_HISTORY = """SELECT hi.clock, hi.value, h.host, i.key_ FROM
                       %s AS hi
                       INNER JOIN items AS i ON hi.itemid = i.itemid
                       INNER JOIN hosts AS h ON i.hostid = h.hostid
                       WHERE hi.itemid IN (%s) AND hi.clock >= %s LIMIT %s"""


def read_last_clock_file(file):
    """Return the clock (Unixtime) from the specified file or current time
if the file doesn't exist"""
    last_clock = int(time.time())
    if os.path.isfile(file):
        try:
            last_clock_file = open(file, 'r')
            last_clock = int(last_clock_file.readline())
            last_clock_file.close()
        except IOError:
            sys.stderr.write('Error: failed to read last clock file, ' +
                             file + '\n')
            sys.exit(2)
    else:
        print('Error: ' + file +
              ' file not found! Starting from current timestamp')
    return last_clock


def write_last_clock_file(file, clock):
    """Write the supplied clock (Unixtime) to file"""
    try:
        file = open(file, 'w')
        file.write(str(clock))
        file.close()
    except IOError:
        sys.stderr.write('Error writing last clock to file: ' +
                         file + '\n')
        sys.exit(2)


def get_db_connection():
    """Get a connection to the DB. We do this per query to ensure we always
have a live connection."""
    conn = mysql.connector.connect(user=DB_UNAME, password=DB_PW,
                                   host=DB_SERVER, database=DB_DATABASE,
                                   autocommit=True)
    return conn


def get_wavefront_client():
    """Connect to the Wavefront Proxy. We do this per connection to ensure we
always have a live connection."""
    client_factory = WavefrontClientFactory()
    client_factory.add_client(
        url="proxy://{}:{}".format(WAVEFRONT_PROXY_HOST, WAVEFRONT_PROXY_PORT),
        max_queue_size=50000,
        batch_size=10000,
        flush_interval_seconds=5)
    wavefront_sender = client_factory.get_client()
    return wavefront_sender


def fetch_next_metrics(history_clock, historyuint_clock, wavefront_sender):
    """Send the next batch of Floating point and Integer metrics to the Wavefront
Proxy and return the last clock time for int and float metrics as a tuple.

Query both the history and history_uint tables."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Process the history table which contains floating point metrics
    float_rows = query_db(HISTORY_TABLE, history_clock, cursor)
    float_points_count = len(float_rows)
    history_clock = process_and_send_metrics(
        float_rows, history_clock, wavefront_sender)

    # Process the history_uint table which contains integer metrics
    int_rows = query_db(HISTORY_TABLE_UINT, historyuint_clock, cursor)
    int_points_count = len(int_rows)
    historyuint_clock = process_and_send_metrics(
        int_rows,
        historyuint_clock,
        wavefront_sender)

    cursor.close()
    conn.close()

    print("Processed {} Float Points and {} Integer Points. Press C-c to terminate.".format(
        float_points_count, int_points_count))
    return (history_clock, historyuint_clock)


def query_db(history_table_name, clock, cursor):
    """Query the DB for the given table and clock time and return any rows"""
    cursor.execute(SQL_QUERY_ITEMIDS)

    # The Python MySQL connector doesn't natively handle binding a list
    # to a SQL IN clause so that is handled here add the correct number of %s
    # symbols to the SQL string
    itemids = [x[0] for x in cursor.fetchall()]
    in_p = ','.join(itertools.repeat('%s', len(itemids)))
    sql = SQL_QUERY_HISTORY % (history_table_name, in_p, clock, LIMIT)
    cursor.execute(sql, itemids)

    return cursor.fetchall()


def process_and_send_metrics(rows, latest_clock, wavefront_sender, tags):
    """Convert each row in rows into the Wavefront format and send to the
Wavefront proxy. Return the latest clock value found (which will be unchanged
if rows was empty)"""
    for (clock, value, host, itemkey) in rows:
        # These isinstance checks will only return true with Python3.
        # See this issue: http://sourceforge.net/p/mysql-python/bugs/289/
        if isinstance(host, (bytes, bytearray)):
            host = host.decode()
        if isinstance(itemkey, (bytes, bytearray)):
            itemkey = itemkey.decode()

        metric = convert_key_to_wf_metric(itemkey)
        host = replace_punctuation_and_whitespace(host)
        host = host.replace("_", ".")  # Make sure no underscore in host name

        if clock > latest_clock:
            latest_clock = clock
        # Wavefront metric names must include at least one .
        if "." not in metric:
            warning("Cannot process Zabbix item with key_: {} "
                "as it contains no . character".format(itemkey))
            continue

        # wavefront_sender will be None if SEND_TO_WF is False
        if wavefront_sender:
            wavefront_sender.send_metric(metric, value, clock, host, tags)
        else:
            di_msg = "{0}{1} {2} host={3}\n".format(metric,
                                                    value, clock, host)
            print(di_msg)

    return latest_clock


def convert_key_to_wf_metric(key):
    """A Wavefront metric name can be: 'Any lowercase, alphanumeric,
dot-separated value. May also include dashes (-) or underscores (_)'

For the purposes of sending data from Zabbix to Wavefront we replace any
whitespace or punctuation other than dash and underscore from the Zabbix key_
with a single . and convert the whole string to lower case. """
    metric = replace_punctuation_and_whitespace(key)
    metric = remove_trailing_dots(metric)
    metric = just_one_dot(metric)
    metric = prefix_metric(metric)
    metric = metric.lower()
    return metric


def replace_punctuation_and_whitespace(text):
    """Replace occurrences of punctuation (other than . - _) and
any consecutive white space with ."""
    regex = re.compile(r"[^\w\s\-.]|\s+")
    return regex.sub(".", text)


def just_one_dot(text):
    """Some Zabbix metrics can end up with multiple . characters.
Replace with a single one"""
    regex = re.compile(r"\.+")
    return regex.sub(".", text)


def prefix_metric(metric):
    """Apply metric prefix if the collected metrics does
not start with 'zabbix.'"""
    if metric.startswith(ZABBIX_PREFIX):
        return metric
    else:
        return ZABBIX_PREFIX + metric


def remove_trailing_dots(text):
    """Remove any trailing . characters from a metric name"""
    regex = re.compile(r"\.+$")
    return regex.sub("", text)


def format_clock(clock):
    """Return the clock in a human readable format"""
    return datetime.datetime.fromtimestamp(int(clock)).strftime(
        '%Y-%m-%d %H:%M:%S')


def warning(*msgs):
    """Print the supplied msgs to stderr as a warning"""
    print("WARNING: ", *msgs, file=sys.stderr)


def error(*msgs):
    """Print the supplied msgs to stderr as an Error"""
    print("ERROR: ", *msgs, file=sys.stderr)


def signal_handler(signal, frame):
    """Print the final Float Clock Time"""
    print("Wrapping up. Final Float Clock Time: {}. "
        "Final Int Clock Time: {}.".format(history_clock, historyuint_clock))

    write_last_clock_file(HISTORY_CLOCK_FILEPATH, history_clock)
    write_last_clock_file(HISTORYUINT_CLOCK_FILEPATH, historyuint_clock)

    sys.exit(0)

if __name__ == "__main__":
    # The clocks store the epoch time that has already been sent
    # to Wavefront for each Zabbix telemetry table (history and history_uint)
    history_clock = read_last_clock_file(HISTORY_CLOCK_FILEPATH)
    historyuint_clock = read_last_clock_file(HISTORYUINT_CLOCK_FILEPATH)

    # Listen for C-c and exit cleanly
    signal.signal(signal.SIGINT, signal_handler)

    wavefront_sender = None
    if SEND_TO_WF:
        wavefront_sender = get_wavefront_client()

    # Loop forever (unless killed by SIGINT) or exception caught
    while True:
        try:
            history_clock, historyuint_clock = fetch_next_metrics(
                history_clock, historyuint_clock, wavefront_sender)

            msg = "Latest Float Point: {}. Latest Integer Point: {}."
            print(msg.format(
                format_clock(history_clock),
                format_clock(historyuint_clock)))

            write_last_clock_file(HISTORY_CLOCK_FILEPATH, history_clock)
            write_last_clock_file(
                HISTORYUINT_CLOCK_FILEPATH, historyuint_clock)
            time.sleep(POLL_INTERVAL)
        except mysql.connector.Error as e:
            error("Please check your database configuration: {}".format(e))
            sys.exit(1)
        except socket.error as e:
            error("Please check your Wavefront Proxy configuration: {}".format(e))
            if SEND_TO_WF:
                wavefront_sender.flush_now()
                wavefront_sender.close()
            sys.exit(1)
