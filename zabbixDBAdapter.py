#!/usr/bin/python

from __future__ import print_function

import os
import sys
import mysql.connector
import re
import time
import datetime
import socket
import signal
import itertools


# USER CONFIGURATION ###

ZABBIX_PREFIX = "zabbix."    # All metrics will be prefixed with this text. Please ensure it has a trailing .

POLL_INTERVAL = 60           # Frequency at which records will be retrieved from the DB in seconds
LIMIT = 10000                # Limit the amount of records that will be pulled from the DB on each POLL_INTERVAL

SEND_TO_WF = False            # Set to False to print values rather than sending to Wavefront
WAVEFRONT_PROXY_HOST = "localhost"
WAVEFRONT_PROXY_PORT = 2878

DB_SERVER = "localhost"
DB_DATABASE = "zabbix"
DB_UNAME = "root"
DB_PW = "password"

# Store the latest Epoch time that has already been sent to Wavefront in these files. There is a separate file for each
# Zabbix telemetry table - history and history_uint The "history" table stores metrics with float/double values and the
# "history_uint" table stores metrics with Integer values.
HISTORY_CLOCK_FILEPATH = "last_history_clock.hist"
HISTORYUINT_CLOCK_FILEPATH = "last_historyuint_clock.hist"

# END USER CONFIGURATION ###

# SQLs. The results of SQL_QUERY_ITEMIDS are used within SQL_QUERY_HISTORY to force MySQL to use the history_1 index
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
    """Return the clock (Unixtime) from the specified file or current time if the file doesn't exist"""
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
    """Get a connection to the DB. We do this per query to ensure we always have a live
connection."""
    conn = mysql.connector.connect(user=DB_UNAME, password=DB_PW,
                                   host=DB_SERVER, database=DB_DATABASE,
                                   autocommit=True)
    return conn


def get_socket():
    """Connect to the Wavefront Proxy. We do this per connection to ensure we always have a
live connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((WAVEFRONT_PROXY_HOST, WAVEFRONT_PROXY_PORT))
    return s


def fetch_next_metrics(history_clock, historyuint_clock):
    """Send the next batch of Floating point and Integer metrics to the Wavefront
Proxy and return the last clock time for int and float metrics as a tuple.

Query both the history and history_uint tables."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sock = None
    if SEND_TO_WF:
        sock = get_socket()

    # Process the history table which contains floating point metrics
    float_rows = query_db(HISTORY_TABLE, history_clock, cursor)
    float_points_count = len(float_rows)
    history_clock = process_and_send_metrics(float_rows, history_clock, sock)

    # Process the history_uint table which contains integer metrics
    int_rows = query_db(HISTORY_TABLE_UINT, historyuint_clock, cursor)
    int_points_count = len(int_rows)
    historyuint_clock = process_and_send_metrics(int_rows, historyuint_clock, sock)

    cursor.close()
    conn.close()
    if SEND_TO_WF:
        sock.close()

    print("Processed {} Float Points and {} Integer Points. Press C-c to terminate.".format(
        float_points_count, int_points_count))
    return (history_clock, historyuint_clock)


def query_db(history_table_name, clock, cursor):
    """Query the DB for the given table and clock time and return any rows"""
    cursor.execute(SQL_QUERY_ITEMIDS)

    # The Python MySQL connector doesn't natively handle binding a list to a SQL IN clause
    # so that is handled here add the correct number of %s symbols to the SQL string
    itemids = [x[0] for x in cursor.fetchall()]
    in_p = ','.join(itertools.repeat('%s', len(itemids)))
    sql = SQL_QUERY_HISTORY % (history_table_name, in_p, clock, LIMIT)
    cursor.execute(sql, itemids)

    return cursor.fetchall()


def process_and_send_metrics(rows, latest_clock, sock=None):
    """Convert each row in rows into the Wavefront format and send to the Wavefront
proxy. Return the latest clock value found (which will be unchanged if rows was empty)"""
    for (clock, value, host, itemkey) in rows:
        # These isinstance checks will only return true with Python3. See this issue:
        # http://sourceforge.net/p/mysql-python/bugs/289/
        if isinstance(host, bytes):
            host = host.decode()
        if isinstance(itemkey, bytes):
            itemkey = itemkey.decode()

        metric = convert_key_to_wf_metric(itemkey)
        host = replace_punctuation_and_whitespace(host)
        host = host.replace("_", ".")  # Make sure no underscore in host name

        if metric.startswith("zabbix."):
            di_msg = "{0}{1} {2} {3} host={4}\n".format("", metric,
                                                    value, clock, host)
        else:
            di_msg = "{0}{1} {2} {3} host={4}\n".format(ZABBIX_PREFIX, metric,
                                                    value, clock, host)

        if clock > latest_clock:
            latest_clock = clock
        # Wavefront metric names must include at least one .
        if "." not in di_msg:
            warning("Cannot process Zabbix item with key_: {} as it contains no . character".format(itemkey))
            continue

        # sock will be None if configuration option SEND_TO_WF is False
        if sock:
            sock.sendall(di_msg.encode('utf-8'))
        else:
            print(di_msg)

    return latest_clock


def convert_key_to_wf_metric(key):
    """A Wavefront metric name can be: 'Any lowercase, alphanumeric, dot-separated value. May also
include dashes (-) or underscores (_)'

For the purposes of sending data from Zabbix to Wavefront we replace any whitespace or punctuation
other than dash and underscore from the Zabbix key_ with a single . and convert the whole string to
lower case. """
    metric = replace_punctuation_and_whitespace(key)
    metric = remove_trailing_dots(metric)
    metric = just_one_dot(metric)
    metric = metric.lower()
    return metric


def replace_punctuation_and_whitespace(text):
    """Replace occurrences of punctuation (other than . - _) and any consecutive white space with ."""
    rx = re.compile(r"[^\w\s\-.]|\s+")
    return rx.sub(".", text)


def just_one_dot(text):
    """Some Zabbix metrics can end up with multiple . characters. Replace with a single one"""
    rx = re.compile(r"\.+")
    return rx.sub(".", text)


def remove_trailing_dots(text):
    """Remove any trailing . characters from a metric name"""
    rx = re.compile(r"\.+$")
    return rx.sub("", text)


def format_clock(clock):
    """Return the clock in a human readable format"""
    return datetime.datetime.fromtimestamp(int(clock)).strftime('%Y-%m-%d %H:%M:%S')


def warning(*msgs):
    """Print the supplied msgs to stderr as a warning"""
    print("WARNING: ", *msgs, file=sys.stderr)


def error(*msgs):
    """Print the supplied msgs to stderr as an Error"""
    print("ERROR: ", *msgs, file=sys.stderr)


def signal_handler(signal, frame):
    print("Wrapping up. Final Float Clock Time: {}. Final Int Clock Time: {}.".format(
        history_clock, historyuint_clock))

    write_last_clock_file(HISTORY_CLOCK_FILEPATH, history_clock)
    write_last_clock_file(HISTORYUINT_CLOCK_FILEPATH, historyuint_clock)

    sys.exit(0)

if __name__ == "__main__":
    # The clocks store the epoch time that has already been sent to Wavefront for each Zabbix
    # telemetry table (history and history_uint)
    history_clock = read_last_clock_file(HISTORY_CLOCK_FILEPATH)
    historyuint_clock = read_last_clock_file(HISTORYUINT_CLOCK_FILEPATH)

    # Listen for C-c and exit cleanly
    signal.signal(signal.SIGINT, signal_handler)

    # Loop forever (unless killed by SIGINT) or exception caught
    while(True):
        try:
            history_clock, historyuint_clock = fetch_next_metrics(history_clock, historyuint_clock)

            msg = "Latest Float Point: {}. Latest Integer Point: {}."
            print(msg.format(format_clock(history_clock), format_clock(historyuint_clock)))

            write_last_clock_file(HISTORY_CLOCK_FILEPATH, history_clock)
            write_last_clock_file(HISTORYUINT_CLOCK_FILEPATH, historyuint_clock)
            time.sleep(POLL_INTERVAL)
        except mysql.connector.Error as e:
            error("Please check your database configuration: {}".format(e))
            sys.exit(1)
        except socket.error as e:
            error("Please check your Wavefront Proxy configuration: {}".format(e))
            sys.exit(1)
