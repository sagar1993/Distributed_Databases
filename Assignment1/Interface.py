#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import csv
import tempfile

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'test_data.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 20
META_DATA_TABLE = 'meta_data'



def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    create_meta_data_table(openconnection)

    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    command = 'CREATE TABLE IF NOT EXISTS %s ( userid INTEGER,  movieid NUMERIC NOT NULL, rating NUMERIC NOT NULL )' % (ratingstablename,)
    cur.execute(command)

    with open(ratingsfilepath, 'r') as f:
    
        fp = tempfile.NamedTemporaryFile()
        reader = csv.reader((line.replace('::', ':') for line in f), delimiter=':')
        for row in reader:
            fp.write('\t'.join(row[:-1])+'\n')
        fp.seek(0)
        cur.copy_from(fp,'ratings')

    cur.close()



def rangepartition(ratingstablename, numberofpartitions, openconnection):

    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    insert_meta_data_table(openconnection, ratingstablename, RANGE_TABLE_PREFIX, numberofpartitions, 0)

    max_rating = 5.0
    n = numberofpartitions
    start, end = -1, 0
    index = 0
    length = max_rating / n

    while end < max_rating:
        end += length
        tablename = RANGE_TABLE_PREFIX + str(index)

        command = 'CREATE TABLE IF NOT EXISTS %s ( userid INTEGER,  movieid NUMERIC NOT NULL, rating NUMERIC NOT NULL )' % (tablename,)
        cur.execute(command)

        command_insert = 'INSERT INTO %s SELECT * FROM %s WHERE rating > %s and rating <= %s' % (tablename, ratingstablename, start, end)

        cur.execute(command_insert)

        index += 1
        start = end
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

    RROBIN_TABLE_PREFIX = 'rrobin_part'

    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    for i in range(numberofpartitions):
        tablename = RROBIN_TABLE_PREFIX + str(i)

        command = 'CREATE TABLE IF NOT EXISTS %s ( userid INTEGER,  movieid NUMERIC NOT NULL, rating NUMERIC NOT NULL )' % (tablename,)
        cur.execute(command)

        command_insert = 'INSERT INTO %s SELECT T.userid, T.movieid, T.rating FROM ( SELECT ROW_NUMBER() OVER() as row_number, * from %s ) T WHERE  MOD(T.row_number, %s ) = %s' % (
        tablename, ratingstablename, numberofpartitions, i,)
        cur.execute(command_insert)

    cur.execute('SELECT COUNT(*) FROM %s' % (ratingstablename,))
    count = cur.fetchone()[0]

    insert_meta_data_table(openconnection, ratingstablename, RROBIN_TABLE_PREFIX, numberofpartitions, count)

    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    # get count and number of partition from meta data table
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    number, current = select_meta_data_table(openconnection, ratingstablename, RROBIN_TABLE_PREFIX)

    # current += 1
    table_index = current % number
    table_name = RROBIN_TABLE_PREFIX + str(table_index)

    # insert
    command = 'INSERT INTO %s VALUES (%s, %s, %s)' % (table_name, userid, itemid, rating)
    cur.execute(command)

    # update meta data
    update_meta_data_table(openconnection, ratingstablename, RROBIN_TABLE_PREFIX, current+1)


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    partition_name = 'range_part'
    max_rating = 5.0

    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    # select number of partition
    cur.execute(
        'SELECT number FROM meta_data WHERE partition_name=\'%s\' and tablename=\'%s\'' % (partition_name, 'ratings'))
    number = int(cur.fetchone()[0])

    divide = max_rating / number

    partition_index = int(rating // divide)

    if partition_index == rating:
        partition_index -= 1

    table_name = partition_name + str(partition_index)

    command = 'INSERT INTO %s VALUES (%s, %s, %s)' % (table_name, userid, itemid, rating)
    cur.execute(command)

    cur.close()


def deletepartitionsandexit(openconnection):
    pass


def create_meta_data_table(openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    command = 'CREATE TABLE IF NOT EXISTS %s ( tablename Varchar(20),  partition_name varchar(20), number NUMERIC, current NUMERIC)'% (META_DATA_TABLE,)
    cur.execute(command)

    cur.close()

def insert_meta_data_table(openconnection, tablename, partition_name, number=0, current=0):

    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    # tablename and partition name
    cur.execute('SELECT COUNT(*) FROM meta_data WHERE partition_name=\'%s\'' % (partition_name,))
    count = cur.fetchone()[0]

    if count == 0:
        command = 'INSERT INTO %s VALUES (\'%s\', \'%s\', %s, %s)' % (META_DATA_TABLE, tablename, partition_name, number, current)
        cur.execute(command)

    cur.close()

def select_meta_data_table(openconnection, tablename, partition_name):

    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    cur.execute('SELECT number, current FROM meta_data WHERE tablename=\'%s\' and partition_name=\'%s\'' % (
    tablename, partition_name,))
    data = cur.fetchone()

    cur.close()
    return data

def update_meta_data_table(openconnection, tablename, partition_name, current):

    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    command = 'UPDATE meta_data SET current = %s WHERE tablename=\'%s\' and partition_name=\'%s\'' % (current, tablename, partition_name,)
    cur.execute(command)
    cur.close()



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()
