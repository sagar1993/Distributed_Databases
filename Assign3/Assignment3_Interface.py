import psycopg2
import os
import sys
import threading

FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'

THREADS = 5
RANGE_PARTITION = "range"
JOIN_RANGE_PARTITION = "joinrange"
TABLE1_RANGE_PARTITION = "t1_range"
TABLE2_RANGE_PARTITION = "t2_range"

def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):

    cur = openconnection.cursor()

    q = "select  min({0}), max({0}) from {1} ".format(SortingColumnName, InputTable)
    cur.execute(q)
    min_val, max_val = cur.fetchone()

    length = abs(max_val - min_val) * 1.0 / THREADS

    for i in range(THREADS):
        outputname = RANGE_PARTITION + str(i)
        createTable(InputTable, outputname, cur)

    createTable(InputTable, OutputTable, cur)

    threads = range(THREADS)

    for i in range(THREADS):
        if i == 0:
            start = min_val
            end = min_val + length
        else:
            start = end
            end = end + length
        rangetable = RANGE_PARTITION + str(i)
        threads[i] = threading.Thread(target=parallelSort, args=(InputTable, rangetable, SortingColumnName, start, end, openconnection))

        threads[i].start()

    for i in range(THREADS):
        threads[i].join()

    for i in range(THREADS):
        tablename = RANGE_PARTITION + str(i)
        q = "INSERT INTO {0} SELECT * FROM {1}".format(OutputTable, tablename)
        cur.execute(q)

    for i in range(THREADS):
        tablename = RANGE_PARTITION + str(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(tablename))

    openconnection.commit()


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()

    q = "select  min({0}), max({0}) from {1} ".format(Table1JoinColumn, InputTable1)
    cur.execute(q)
    min1, max1 = cur.fetchone()

    q = "select  min({0}), max({0}) from {1} ".format(Table2JoinColumn, InputTable2)
    cur.execute(q)
    min2, max2 = cur.fetchone()

    min_val = min(min1, min2)
    max_val = max(max1, max2)

    length = abs(max_val - min_val) * 1.0 / THREADS
    rangePartition(InputTable1, Table1JoinColumn, length, min_val, max_val, TABLE1_RANGE_PARTITION, cur)
    rangePartition(InputTable2, Table2JoinColumn, length, min_val, max_val, TABLE2_RANGE_PARTITION, cur)

    for i in range(THREADS):
        outputname = JOIN_RANGE_PARTITION + repr(i)
        createJoinTable(InputTable1, InputTable2, outputname, cur)

    threads = range(THREADS)

    for i in range(THREADS):
        table1 = TABLE1_RANGE_PARTITION + str(i)
        table2 = TABLE2_RANGE_PARTITION + str(i)
        output = JOIN_RANGE_PARTITION + str(i)

        threads[i] = threading.Thread(target=parallelJoin, args=(table1, table2, Table1JoinColumn, Table2JoinColumn, output, openconnection))

        threads[i].start()

    for i in range(THREADS):
        threads[i].join()

    createJoinTable(InputTable1, InputTable2, OutputTable, cur)

    for i in range(THREADS):
        table = JOIN_RANGE_PARTITION + str(i)
        q = "INSERT INTO {0} SELECT * FROM {1}".format(OutputTable, table)
        cur.execute(q)

    for i in range(THREADS):
        table1 = TABLE1_RANGE_PARTITION + str(i)
        table2 = TABLE2_RANGE_PARTITION + str(i)
        table3 = JOIN_RANGE_PARTITION + str(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table1))
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table2))
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table3))
    openconnection.commit()


def createTable(sourcetable, destinationtable, cur):
    q = "CREATE TABLE {0} AS SELECT * FROM {1} WHERE 1=2".format(destinationtable, sourcetable)
    cur.execute(q)


def parallelSort(InputTable, rangetable, SortingColumnName, min_val, max_val, openconnection):
    cur = openconnection.cursor()
    if rangetable == 'range0':
        q = "INSERT INTO {0} SELECT * FROM {1}  WHERE {2} >= {3}  AND {2} <= {4} ORDER BY {2} ASC".format(
            rangetable, InputTable,
            SortingColumnName, min_val, max_val)
    else:
        q = "INSERT INTO {0}  SELECT * FROM {1}  WHERE {2}  > {3}  AND {2}  <= {4} ORDER BY {2} ASC".format(
            rangetable, InputTable,
            SortingColumnName, min_val, max_val)
    cur.execute(q)


def rangePartition(inputtable, tablejoincolumn, length, min_val, max_val, tableprefix, cur):
    for i in range(THREADS):
        tablename = tableprefix + str(i)
        if i == 0:
            min_val = min_val
            max_val = min_val + length
            q = "CREATE TABLE {0} AS  SELECT * FROM {1}  WHERE {2} >= {3} AND {2} <= {4}".format(tablename, inputtable, tablejoincolumn, min_val, max_val)
        else:
            min_val = max_val
            max_val = min_val + length
            q = "CREATE TABLE {0} AS  SELECT * FROM {1}  WHERE {2} > {3} AND {2} <= {4}".format(tablename, inputtable, tablejoincolumn, min_val, max_val)
        cur.execute(q)


def createJoinTable(table1, table2, outputname, cur):
    q = "CREATE TABLE {0} AS SELECT * FROM {1},{2} WHERE 1=2".format(outputname, table1, table2)
    cur.execute(q)

def parallelJoin(table1, table2, Table1JoinColumn, Table2JoinColumn, output, openconnection):
    cur = openconnection.cursor()
    q = "insert into {0} select * from {1} INNER JOIN {2} ON {1}.{3} = {2}.{4}".format(output, table1, table2, Table1JoinColumn, Table2JoinColumn)
    cur.execute(q)


def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")



def createDB(dbname='ddsassignment3'):

    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))
    else:
        print 'A database named {0} already exists'.format(dbname)

    cur.close()
    con.commit()
    con.close()


def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" % (ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d` + ",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:

        print "Creating Database named as ddsassignment3"
        createDB()

        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection()

        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con)

        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE,
                     'parallelJoinOutputTable', con)

        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con)
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con)

        deleteTables('parallelSortOutputTable', con)
        deleteTables('parallelJoinOutputTable', con)

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
