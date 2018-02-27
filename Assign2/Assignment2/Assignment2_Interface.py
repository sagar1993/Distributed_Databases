#!/usr/bin/python2.7

import psycopg2
import os
import sys


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    RANGE_QUERY_OUTPUT_FILE = 'RangeQueryOut.txt'

    RANGE_METADATA = 'range' + ratingsTableName + 'metadata'
    ROUND_ROBIN_METADATA = 'roundrobin' + ratingsTableName + 'metadata'

    RANGE_PARTITION_OUTPUT_NAME = 'Range' + ratingsTableName.title() + 'Part'
    ROUND_ROBIN_PARTITION_OUTPUT_NAME = 'RoundRobin' + ratingsTableName.title() + 'Part'


    try:
        cursor = openconnection.cursor()

        statement = "select  max(minrating) from {0} where minrating <= {1}".format(RANGE_METADATA,ratingMinValue)
        cursor.execute(statement)
        min_partition_number = cursor.fetchone()[0]

        statement = "select  min(maxrating) from {0} where maxrating >= {1}".format(RANGE_METADATA,ratingMaxValue)
        cursor.execute(statement)
        max_partition_number = cursor.fetchone()[0]

        statement = "select  partitionnum from {0} where maxrating >= {1} and maxrating <= {2}".format(RANGE_METADATA, min_partition_number, max_partition_number)
        cursor.execute(statement)
        values = cursor.fetchall()

        if os.path.exists(RANGE_QUERY_OUTPUT_FILE):
            os.remove(RANGE_QUERY_OUTPUT_FILE)

        for i in values:
            partition_name = RANGE_PARTITION_OUTPUT_NAME + str(i[0])
            statement = "select * from {0} where rating >= {1} and rating <= {2}".format(partition_name, ratingMinValue, ratingMaxValue) 
            cursor.execute(statement)
            values2 = cursor.fetchall()
            with open(RANGE_QUERY_OUTPUT_FILE,'a+') as f:
                for j in values2:
                    f.write("%s," % partition_name)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))

        statement = "select partitionnum from {0} ".format(ROUND_ROBIN_METADATA)	
        cursor.execute(statement)
        count = int(cursor.fetchone()[0])
        
        for i in range(count):
            partition_name = ROUND_ROBIN_PARTITION_OUTPUT_NAME + repr(i)
            statement = "select * from {0} where rating >= {1} and rating <= {2}".format(partition_name, ratingMinValue, ratingMaxValue) 
            cursor.execute(statement)
            values2 = cursor.fetchall()
            with open(RANGE_QUERY_OUTPUT_FILE,'a+') as f:
                for j in values2:
                    f.write("%s," % partition_name)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))

    except Exception as e:
        print("Range Query Exception", e)


def PointQuery(ratingsTableName, ratingValue, openconnection):

    POINT_QUERY_OUTPUT_FILE = 'PointQueryOut.txt'

    RANGE_METADATA = 'range' + ratingsTableName + 'metadata'
    ROUND_ROBIN_METADATA = 'roundrobin' + ratingsTableName + 'metadata'


    RANGE_PARTITION_OUTPUT_NAME = 'Range' + ratingsTableName.title() + 'Part'
    ROUND_ROBIN_PARTITION_OUTPUT_NAME = 'RoundRobin' + ratingsTableName.title() + 'Part'

    try:

        cursor = openconnection.cursor()
        partition_number = 0 
        if ratingValue != 0:
            statement = "select partitionnum from {0} where minrating < {1} and maxrating >= {1}".format(RANGE_METADATA,ratingValue)
            cursor.execute(statement)
            partition_number = cursor.fetchone()[0]

        partition_name = RANGE_PARTITION_OUTPUT_NAME + str(partition_number)
        statement = "select * from {0} where rating = {1} ".format(partition_name, ratingValue ) 
        cursor.execute(statement)
        values2 = cursor.fetchall()

        if os.path.exists(POINT_QUERY_OUTPUT_FILE):
            os.remove(POINT_QUERY_OUTPUT_FILE)

        with open(POINT_QUERY_OUTPUT_FILE,'a+') as f:
            for j in values2:
                f.write("%s," % partition_name)
                f.write("%s," % str(j[0]))
                f.write("%s," % str(j[1]))
                f.write("%s\n" % str(j[2]))

        statement = "select partitionnum from {0} ".format(ROUND_ROBIN_METADATA)	
        cursor.execute(statement)
        count = int(cursor.fetchone()[0])
        
        for i in range(count):
            partition_name = ROUND_ROBIN_PARTITION_OUTPUT_NAME + repr(i)
            statement = "select * from {0} where rating = {1} ".format(partition_name, ratingValue ) 
            cursor.execute(statement)
            values2 = cursor.fetchall()
            with open(POINT_QUERY_OUTPUT_FILE,'a+') as f:
                for j in values2:
                    f.write("%s," % partition_name)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))

    except Exception as e:
        print("Point Query Exception",e)
