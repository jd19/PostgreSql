# #!/usr/bin/python2.7
# #
# # Assignment2 Interface
# #
#
# import psycopg2
# import os
# import sys
# # Donot close the connection inside this file i.e. do not perform openconnection.close()
# def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
#     #Implement RangeQuery Here.
#     pass #Remove this once you are done with implementation
#
# def PointQuery(ratingsTableName, ratingValue, openconnection):
#     #Implement PointQuery Here.
#     pass # Remove this once you are done with implementation


#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    #table name: rangeratindgspart
    # roundrobinratingspart
    cursor = openconnection.cursor()
    cursor.execute("select distinct partitionnum from rangeratingsmetadata where maxrating >= {} and minrating <= {}".format(ratingMinValue,ratingMaxValue))
    partitions = cursor.fetchall()


    for i in partitions:
        # Roundrobin parts range query
        # We can also define the number of total partition in range

        query = "COPY (SELECT 'RangeRatingsPart{}' AS userid,userid,movieid,rating FROM RangeRatingsPart{} where rating BETWEEN {} AND {} ) TO STDOUT DELIMITER AS ','".format(i[0],i[0],ratingMinValue,ratingMaxValue)

        with open('RangeQueryOut.txt', 'a') as f:
            cursor.copy_expert(query, f)

            # Range parts range query
    cursor.execute("select partitionnum from roundrobinratingsmetadata")
    noofpartitions = cursor.fetchall()[0][0]


    for i in range(0,noofpartitions):
        query = "COPY( SELECT 'RoundRobinRatingsPart{}' AS userid,userid,movieid,rating FROM RoundRobinRatingsPart{} where rating BETWEEN {} AND {} ) TO STDOUT DELIMITER AS ',' ".format(i,i,ratingMinValue,ratingMaxValue)

        with open('RangeQueryOut.txt', 'a') as f:
            cursor.copy_expert(query, f)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.

    cursor = openconnection.cursor()

    cursor.execute("select distinct partitionnum from rangeratingsmetadata where maxrating>= {} and minrating<= {}".format(ratingValue,ratingValue))
    partitions = cursor.fetchall()

    for i in partitions:
    # range parts point query
    # We can also define the number of total partition in range

        query = "COPY ( SELECT 'RangeRatingsPart{}' AS userid,userid,movieid,rating FROM rangeratingspart{} where rating = {} ) TO STDOUT DELIMITER AS ','".format(i[0],i[0],ratingValue)

        with open('PointQueryOut.txt', 'a') as f:
            cursor.copy_expert(query, f)

  #Roundrobin parts point query
    cursor.execute("select partitionnum from roundrobinratingsmetadata")
    noofpartitions = cursor.fetchall()[0][0]

    for i in range(0,noofpartitions):
        query = "COPY ( SELECT 'RoundRobinRatingsPart{}' AS userid,userid,movieid,rating FROM RoundRobinRatingsPart{} where rating = {} ) TO STDOUT DELIMITER AS ','".format(i,i,ratingValue)


        with open('PointQueryOut.txt', 'a') as f:
            cursor.copy_expert(query, f)
