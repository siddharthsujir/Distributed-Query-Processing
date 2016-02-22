
import psycopg2
import re

DATABASE_NAME = 'ddb_assignment'
n=0
loc=0


def getopenconnection(user='postgres', password='xxxxxxx', dbname=DATABASE_NAME):#dds_assgn1
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    conn=openconnection
    cur = conn.cursor()
    cur.execute("select count(*) from pg_tables where tablename='{0}'".format(ratingstablename.lower()))
    count = cur.fetchone()[0]
    if count == 0:
        print("This may take few minutes (2-4 minutes). Please wait")
        #create temporary table
        cur.execute("drop table if exists newTest1")
        cur.execute("CREATE TABLE newTest1 (temp varchar)")
        cur.execute(r"COPY newTest1 FROM '{0}'".format(ratingsfilepath)) #C:\downloads\ml-10M100K\ratings.dat
        #create new table
        cur.execute("drop table if exists {0}".format(ratingstablename))
        cur.execute("SELECT * INTO {0} FROM (SELECT cast(split_part(temp, '::', 1) as int) AS UserId , cast(split_part(temp, '::', 2)as int) AS MovieId , cast(split_part(temp, '::', 3) as float )AS Rating , split_part(temp, '::', 4) AS Random FROM  newTest1) as foo".format(ratingstablename))
        #drop temporary table
        cur.execute("DROP TABLE newTest1")
        #Drop last column
        cur.execute("ALTER TABLE {0} DROP COLUMN Random".format(ratingstablename))
        print("Done: Data Loaded into Table")
    else:
        print ("A Table named {0} already exists".format(ratingstablename))
    conn.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if(float(numberofpartitions%1)==0 and numberofpartitions>=0):
        conn=openconnection
        global n
        n=numberofpartitions
        cur = conn.cursor()
        if(int(numberofpartitions)==0):
            print("0 is not a valid entry")
            return 0;
        elif(int(numberofpartitions)==1):
             print("Enter more than 1 fragment!")
             return 0;
        else:
            split = 5/int(numberofpartitions)
            ranges=float(split);
            offset =float(0)
            #creating sub-table for parent table rating
            Delete_Partitions(openconnection)

            for val in range(0,int(numberofpartitions)):

                if(val == int(numberofpartitions)-1 and int(numberofpartitions)%2 != 0):
                    ranges+=split
                print("Doing Partition",val+1)
                cur.execute("SELECT * INTO PARTITION{0} FROM (SELECT * FROM {1} where Rating >{2} AND Rating<={3}) as foo".format(val+1,ratingstablename,offset,ranges))
                print("done")
                offset=ranges
                ranges+=split
                conn.commit()
        cur.execute("Insert INTO PARTITION1 SELECT * FROM {0} where Rating=0".format(ratingstablename))
        conn.commit()
        cur.close()
    else:
        print("Partition should be an Integer")


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if(float(numberofpartitions%1)==0 and numberofpartitions>=0):
        conn = openconnection
        global n
        n=numberofpartitions
        cur = conn.cursor()
        if(int(numberofpartitions)==0):
            print("0 is not a valid entry")
            return 0;
        elif(int(numberofpartitions)==1):
             print("Enter more than 1 fragment!")
             return 0;
        else:
            newn = int(numberofpartitions)-1
            val=0;
            Delete_Partitions(openconnection)
            while newn>=0 and val in range(0,int(numberofpartitions)):

                print ("Partitionss{0} done. ".format(val))
                print("SELECT * INTO PARTITIONSS{0} FROM (SELECT UserId,MovieId,Rating FROM ( SELECT *, {2}+row_number() over () AS number FROM {3} ) foo WHERE foo.number % {1} = 0) as t;".format(val,numberofpartitions,newn,ratingstablename))
                cur.execute("SELECT * INTO PARTITIONSS{0} FROM (SELECT UserId,MovieId,Rating FROM ( SELECT *, {2}+row_number() over () AS number FROM {3} ) foo WHERE foo.number % {1} = 0) as t;".format(val,numberofpartitions,newn,ratingstablename))
                newn+=1
                val+=1
        conn.commit()
        cur.close()
    else:
        print("Partition should be an Integer")


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if(rating<=5 and rating>=0 and rating%(.5)==0):
        conn = openconnection
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM {}".format(ratingstablename))
        for row in cur.fetchall():
            max = str(row[0])
        global n
        if(n==0 or n==1):
            print("Not proper partitioning values!")
            return
        tableNo = int(max)%int(n)
        if(tableNo==int(n)-1):
            tableNo=1
        else:
            tableNo+=1
        partition= "partitionss"+str(tableNo)
        var2="INSERT into {0} values({1},{2},{3})".format(partition,int(userid),int(itemid),int(rating))
        try:
            cur.execute(str(var2))
            var1="INSERT INTO {0} values({1},{2},{3})".format(ratingstablename,int(userid),int(itemid),int(rating))
            print(var1)
            cur.execute(str(var1))
            print("Inserted into: {0}".format(partition))
        except:
            print("Partition Doesnt exist. Must partition first")
        conn.commit()
        cur.close()
    else:
        print("Rating: Range out of limit (1..5)")


def rangeinsert ( ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    global n
    if(n==0 or n==1):
        print("Not proper partitioning values!")
        return
    splitVal= 5/int(n)
    #ratingValue/split

    if(rating==5 ):
        temp="partition"+str(int(n))
        var1="INSERT INTO {0} values({1},{2},{3})".format(ratingstablename,int(userid),int(itemid),int(rating))
        print(var1)
        cur.execute(str(var1))
        #insert into last table partition
        cur.execute("Insert into {0} values({1},{2},{3})".format(temp,int(userid),int(itemid),int(rating)))
    elif(rating<5 and rating>=0 and rating%(.5)==0):
        var1="INSERT INTO {0} values({1},{2},{3})".format(ratingstablename,int(userid),int(itemid),int(rating))
        print(var1)
        cur.execute(str(var1))
        tableNo = int(int(rating)/splitVal)
        temp1="partition"+str(tableNo)
        try:
            cur.execute("Insert into {0} values({1},{2},{3})".format(temp1,int(userid),int(itemid),int(rating)))
            print("Inserted into {0}".format(temp1))
        except:
            print("Partition Doesnt exist. Must partition first")
    else:
        print("Rating: Range out of limit (1..5)")
    conn.commit()
    cur.close()

def Delete_Partitions(openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute('drop table if exists tempo')
    cur.execute('create table tempo(columns varchar)')
    cur.execute("Insert into tempo SELECT 'DROP TABLE ' || t.oid::regclass || ';' FROM   pg_class t WHERE  t.relkind = 'r' AND    t.relname ~~ E'partition\_%' ORDER  BY 1")
    cur.execute("select count(*) from tempo")
    count = cur.fetchone()[0]
    val=int(0)
    while(val<int(count)):
        cur.execute("select columns from tempo")
        var= cur.fetchall()[int(val)]
        var = re.sub(r'\W', ' ', str(var))
        print(var)
        val+=1
        cur.execute(str(var))
    cur.execute('Drop table tempo')
    conn.commit()
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
        print ("A database named {0} already exists".format(dbname))

    # Clean up
    cur.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass

def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass

def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    # if(openconnection!='\0'):
    #     openconnection.close()
    pass

#rangequery('Ratings', 3, 4, con)
def RangeQuery(ratingstablename,RatingMinValue,RatingMaxValue,openconnection):
    conn = openconnection
    cur = conn.cursor()
    #query for finding values between min and max
    cur.execute("drop table if exists outputtable")
    cur.execute("create table outputtable(PartitionID varchar,UserId varchar, MovieId varchar,Rating varchar)")
    #select only those paritions pertaining to the value
    cur.execute("drop table if exists value")
    cur.execute('create table value(columns varchar)')
    cur.execute("Insert into value SELECT '' || t.oid::regclass || ';' FROM   pg_class t WHERE  t.relkind = 'r' AND    t.relname ~~ E'partition\_%' ORDER  BY 1")
    cur.execute("select count(*) from value")
    count = cur.fetchone()[0]
    val=int(0)
    print(count)
    while(val<int(count)):
        cur.execute("select columns from value")
        var= cur.fetchall()[int(val)]
        var = re.sub(r'\W', '', str(var))
        print(var)
        #cur.execute("insert into outputtable(PartitionID) values('{0}')".format(var))
        cur.execute("Insert INTO outputtable (partitionid,UserId,MovieId,Rating) SELECT '{0}',UserId,MovieId,Rating FROM {0} where Rating >={1} AND Rating<={2}".format(var,RatingMinValue,RatingMaxValue))
        #cur.execute(r"update outputtable set PartitionId='{0}' where PartitionId ISNULL".format(var))
        print("Finished.. proceeding with next partition")
        val+=1
    #store it in a table
    h = open('rangequery.txt','w')
    cur.copy_to(h,'outputtable','-')
    conn.commit()
    cur.close()

def PointQuery(ratingstablename,RatingValue,openconnection):
    conn = openconnection
    cur = conn.cursor()
    #query for finding values between min and max
    cur.execute("drop table if exists outputtable")
    cur.execute("create table outputtable(PartitionID varchar,UserId varchar, MovieId varchar,Rating varchar)")
    #select only those paritions pertaining to the value
    cur.execute("drop table if exists value")
    cur.execute('create table value(columns varchar)')
    cur.execute("Insert into value SELECT '' || t.oid::regclass || ';' FROM   pg_class t WHERE  t.relkind = 'r' AND    t.relname ~~ E'partition\_%' ORDER  BY 1")
    cur.execute("select count(*) from value")

    count = cur.fetchone()[0]
    val=int(0)
    print(count)
    while(val<int(count)):
        cur.execute("select columns from value")
        var= cur.fetchall()[int(val)]
        var = re.sub(r'\W', '', str(var))
        print(var)
        #cur.execute("insert into outputtable(PartitionID) values('{0}')".format(var))
        cur.execute("Insert INTO outputtable (partitionid,UserId,MovieId,Rating) SELECT '{0}',UserId,MovieId,Rating FROM {0} where Rating ={1}".format(var,RatingValue))
        #cur.execute(r"update outputtable set PartitionId='{0}' where PartitionId ISNULL".format(var))
        print("Finished.. proceeding with next partition")
        val+=1

    #store it in a table


    h = open('pointquery.txt','w')
    cur.copy_to(h,'outputtable','-')
    conn.commit()
    cur.close()



if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()
        create_db(DATABASE_NAME)
        after_db_creation_middleware(DATABASE_NAME)
        con = getopenconnection()

        # Load ratings table: Enter your custom file location
        #loadratings("Ratings",r"/home/system/Assignment_Dataset", con)
        #Partition the table using Round robin method
        roundrobinpartition("Ratings", 5, con)

        #rangepartition("Ratings", 5, con)

        #Call Range query with Min value 3 and Max value 4
        RangeQuery("Ratings",3,4,con)

        #Call Point query with rating value 3
        PointQuery("Ratings",4,con)


            # Use this function to do any set up before I starting calling your functions to test, if you want to
           # before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
       # RangeQuery("ratings",con,"0")

            
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
        after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print ("OOPS! This is the error: ",detail)
