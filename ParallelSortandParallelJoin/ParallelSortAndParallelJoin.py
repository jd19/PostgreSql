#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import sys
import threading
import psycopg2

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


def thread_ParallelSort(InputTable, SortingColumnName, i, start, end, openconnection):
    c = openconnection.cursor()
    if i == 0:
        c.execute("INSERT INTO Sort_Thread{} SELECT * FROM {}  WHERE {} >= {}  AND {} <= {} ORDER BY {} ASC".format(i,InputTable,SortingColumnName,start,SortingColumnName,end,SortingColumnName))
    else:
        c.execute("INSERT INTO Sort_Thread{} SELECT * FROM {}  WHERE {} > {}  AND {} <= {} ORDER BY {} ASC".format(i,InputTable,SortingColumnName,start,SortingColumnName,end,SortingColumnName))

    c.close()
    return

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.

    c = openconnection.cursor()
    # s1 = "SELECT MIN(SortingColumnName),MAX(SortingColumnName) FROM {} ".format(SortingColumnName,SortingColumnName,InputTable)
    c.execute("SELECT MIN({}),MAX({}) FROM {} ".format(SortingColumnName,SortingColumnName,InputTable))
    r = c.fetchall()
    min= (float)(r[0][0])
    max = (float)(r[0][1])
    Range = (max - min)/5.0
    thread = [0,0,0,0,0]

    for i in range(0,5):
        c.execute("DROP TABLE IF EXISTS Sort_Thread{}".format(i))
        c.execute("CREATE TABLE Sort_Thread{} (LIKE {})".format(i,InputTable))

        start = min
        end = min +Range

        thread[i] = threading.Thread(target=thread_ParallelSort,args=(InputTable, SortingColumnName, i, start, end, openconnection))
        thread[i].start()
        min = end


    for j in thread:
        j.join()

    c.execute("DROP TABLE IF EXISTS {}".format(OutputTable))
    c.execute("CREATE TABLE {} (LIKE {} )".format(OutputTable,InputTable))


    for l in range(0, 5):
        c.execute("INSERT INTO {} SELECT * FROM  Sort_Thread{}".format(OutputTable,l))

    c.close()
    openconnection.commit()


def thread_Paralleljoin(Table1JoinColumn, Table2JoinColumn, openconnection, i):
    c = openconnection.cursor()
    c.execute("INSERT INTO output_table_thread{} SELECT * FROM Join_Table1_Thread{} INNER JOIN Join_Table2_Thread{} ON Join_Table1_Thread{}.{} = Join_Table2_Thread{}.{}".format(i,i,i,i,Table1JoinColumn,i,Table2JoinColumn))
    c.close()


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    c = openconnection.cursor()
    c.execute("SELECT MIN({}),MAX({}) FROM {} ".format(Table1JoinColumn,Table1JoinColumn,InputTable1))
    r1 = c.fetchall()
    c.execute("SELECT MIN({}),MAX({}) FROM {} ".format(Table2JoinColumn,Table2JoinColumn,InputTable2))
    r2 = c.fetchall()
    if (float)(r1[0][0]) < (float)(r2[0][0]):
        min = (float)(r1[0][0])
    else:
        min = (float)(r2[0][0])
    if (float)(r1[0][1]) > (float)(r2[0][1]):
        max = (float)(r1[0][1])
    else:
        max = (float)(r2[0][1])
    Range = (max - min)/5.0
    thread = [0,0,0,0,0]

    for i in range(5):

        c.execute("DROP TABLE IF EXISTS output_table_thread{}".format(i))
        c.execute("create table output_table_thread{} as SELECT * FROM {} INNER JOIN {} ON  {}.{} =  {}.{} where 1=2".format(i,InputTable1,InputTable2,InputTable1,Table1JoinColumn,InputTable2,Table2JoinColumn))

        start = min
        end = min +Range

        c.execute("DROP TABLE IF EXISTS Join_Table1_Thread{}".format(i))
        c.execute("DROP TABLE IF EXISTS Join_Table2_Thread{}".format(i))
        if i == 0:
            c.execute("CREATE TABLE Join_Table1_Thread{} AS SELECT * FROM {}  WHERE {} >= {} AND {} <= {}".format(i,InputTable1,Table1JoinColumn,start,Table1JoinColumn,end ))
            c.execute("CREATE TABLE Join_Table2_Thread{} AS SELECT * FROM {}  WHERE {} >= {} AND {} <= {}".format(i,InputTable2,Table2JoinColumn,start,Table2JoinColumn,end))
        else:
            c.execute("CREATE TABLE Join_Table1_Thread{} AS SELECT * FROM {}  WHERE {} > {} AND {} <= {}".format(i,InputTable1,Table1JoinColumn,start,Table1JoinColumn,end ))
            c.execute("CREATE TABLE Join_Table2_Thread{} AS SELECT * FROM {}  WHERE {} > {} AND {} <= {}".format(i,InputTable2,Table2JoinColumn,start,Table2JoinColumn,end))
        min = end
        thread[i] = threading.Thread(target=thread_Paralleljoin,args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))
        thread[i].start()

    for j in thread:
        j.join()

    c.execute("DROP TABLE IF EXISTS {}".format(OutputTable))
    c.execute(
        "create table {} as SELECT * FROM {} INNER JOIN {} ON  {}.{} =  {}.{} where 1=2".format(OutputTable,InputTable1,InputTable2,InputTable1,Table1JoinColumn,InputTable2,Table2JoinColumn))

    for i in range(5):
        c.execute("INSERT INTO  {} SELECT * FROM output_table_thread{}".format(OutputTable,i))


    openconnection.commit()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    print "Count ",count
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
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

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
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
    	# Creating Database ddsassignment3
    	print "Creating Database named as ddsassignment3"
    	createDB();
    	
    	# Getting connection to the database
    	print "Getting connection from the ddsassignment3 database"
    	con = getOpenConnection();

    	# Calling ParallelSort
    	print "Performing Parallel Sort"
    	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

    	# Calling ParallelJoin
    	print "Performing Parallel Join"
    	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
    	
    	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
    	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
    	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

    	# Deleting parallelSortOutputTable and parallelJoinOutputTable
    	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
