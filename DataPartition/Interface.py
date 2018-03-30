#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute(" CREATE TABLE IF NOT EXISTS {} ( UserID INT,MovieID INT,Rating float );".format(ratingstablename));
    openconnection.commit()
    f = open(ratingsfilepath,'r')
    lines = f.readlines()
    for line in lines:
        row = line.split('::')
        cur.execute("insert into {} (userid, movieid, rating) values ({}, {} ,{});".format(ratingstablename,row[0],row[1],row[2]))

    openconnection.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):

    ranges = 5.0/numberofpartitions
    cur = openconnection.cursor()


    for i in range(numberofpartitions):
        cur.execute("drop table if exists range_part{} ".format(i))
        cur.execute(" CREATE TABLE IF NOT EXISTS range_part{} ( UserID INT,MovieID INT,Rating float );".format(i))
        openconnection.commit()
        if i ==0:
            cur.execute(" INSERT INTO range_part{} SELECT * FROM {} WHERE Rating >= {} and Rating <= {} ".format(i,ratingstablename,0,ranges));
        else:
            cur.execute(" INSERT INTO range_part{} SELECT * FROM {} WHERE Rating >{} and Rating<= {} ".format(i,ratingstablename,(i*ranges),((i*ranges)+ranges)));
    openconnection.commit()

    cur.execute("create table if not exists count_range_partion (partions_count integer)")
    cur.execute("insert into count_range_partion values ({})".format(numberofpartitions))
    openconnection.commit()
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()



    cur.execute("drop table if exists TempTable ")
    cur.execute(" CREATE TABLE tempTable AS select userid, movieid, rating , pr from ( SELECT * , ROW_NUMBER() OVER() pr FROM {} ) a".format(ratingstablename))

    cur.execute("create table if not exists round_robin_partion_count (partions_count integer)")
    cur.execute("insert into round_robin_partion_count values ({})".format(numberofpartitions))

    openconnection.commit()
    for i in range(numberofpartitions):
        cur.execute("drop table if exists rrobin_part{} ".format(i))
        cur.execute("create table rrobin_part{} as select userid, movieid, rating from tempTable where (pr-({}+1)) %5 = 0".format(i,i) )
        openconnection.commit()

    openconnection.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("insert into {} (userid, movieid, rating) values ({},{},{})".format(ratingstablename, userid, itemid, rating))


    cur.execute("select * from round_robin_partion_count")
    partitions = cur.fetchone()[0]

    cur.execute("select count(*) from {}".format(ratingstablename))
    total_records=cur.fetchone()[0]

    insert_partition = (total_records - 1) % partitions


    cur.execute("insert into rrobin_part{} (userid, movieid, rating) values ({},{},{})".format(insert_partition,userid, itemid, rating))
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("insert into {} (userid, movieid, rating) values ({},{},{})".format(ratingstablename, userid, itemid, rating))

    cur.execute("select * from count_range_partion")
    partitions = cur.fetchone()[0]

    ranges = 5.0/partitions

    for i in range(partitions):
        if i==0:
            if rating >=0 and rating <=ranges:
                cur.execute("insert into range_part{} (userid, movieid, rating) values ({},{},{})".format(i,userid, itemid,rating))

        else:
            if rating >(i*ranges) and rating<=((i*ranges)+ranges):
                cur.execute("insert into range_part{} (userid, movieid, rating) values ({},{},{})".format(i,userid, itemid,rating))
    openconnection.commit()
    cur.close()




def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("drop schema public cascade;")
    cur.execute("create schema public;")

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
