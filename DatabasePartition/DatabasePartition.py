#!/usr/bin/python2.7
#
# Interface for the assignement
# Author: Siddharth Sujir Mohan (ASU ID: 1207654769)

import psycopg2

RATINGS_TABLE_NAME = 'Ratings'
noofpartition=0

def getopenconnection(user='postgres', password='__PASSWORD_FOR_USER_POSTGRES__', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Copies Data from a file
# Inserts them into temporary table to remove the multiple characters from the delimiter
# Then inserts it into Ratings table 
def loadratings(ratingstablename, ratingsfilepath, openconnection):
	cur=openconnection.cursor()
	try:
		cur.execute("DROP TABLE IF EXISTS movierating")
		cur.execute("DROP TABLE IF EXISTS ratings")
		cur.execute('''CREATE TABLE movierating(userid int, remove1 char,movieid int,remove2 char,rating numeric,remove3 char,timestamp varchar);''')
		cur.execute("CREATE TABLE %s(userid int,movieid int,rating numeric)"%(ratingstablename))
	except:
		print("Table Error")
	openconnection.commit()
	try:
		cur.execute("COPY movierating from '%s' USING DELIMITERS ':'"%(ratingsfilepath))
	except Exception as detail:
		print("Copy Error ",detail)
	openconnection.commit()
	try:
		cur.execute("insert into ratings(Select userid,movieid,rating from movierating)")
		print("File contents successfully loaded into Ratings table!")
	except:
		print("cant insert ")
	openconnection.commit()

# Creates partitions and inserts the data from the ratings table into partitions depending on the range it belongs to
def rangepartition(ratingstablename, numberofpartitions,openconnection):
	global noofpartition
	noofpartition=float(numberofpartitions)
	if noofpartition.is_integer() is False:
		return
	cur=openconnection.cursor()
	n=noofpartition
	i=1
	range=5/n
	end=range
	start=0
	while i<=n:
		tablename="rangepartition"+str(i)
		#create_query='''CREATE TABLE'''+tablename+'''(userid int,movieid int,rating numeric);'''
		try:			
			cur.execute("DROP TABLE IF EXISTS %s"%(tablename))
			openconnection.commit()
			cur.execute("CREATE TABLE %s(userid int,movieid int,rating numeric)"%(tablename))
			print(tablename+" created")
		except:
			print("Table Creation Error")
		openconnection.commit()
		try:
			if start==0:
				cur.execute("Insert into %s(select * from %s where rating=0)"%(tablename,ratingstablename))
			cur.execute("Insert into %s(select * from %s where rating>%f AND rating<=%f)"%(tablename,ratingstablename,start,end))
			openconnection.commit()
			start=end
			end=end+range
		except:
			print("Insertion Error")
		i=i+1

# Creates partitions and inserts the data from the ratings table into partitions using a Round-Robin approach

def roundrobinpartition(ratingstablename, numberofpartitions,openconnection):
	global noofpartition1
	noofpartition1=float(numberofpartitions)
	if noofpartition1.is_integer() is False:
		return
	cur=openconnection.cursor()
	n=noofpartition1
	i=1
	while i<=n:
		tablename="rrpartition"+str(i)
		#create_query='''CREATE TABLE'''+tablename+'''(userid int,movieid int,rating numeric);'''
		try:			
			# Deletes the table if it already exists
			cur.execute("DROP TABLE IF EXISTS %s"%(tablename))
			openconnection.commit()
			cur.execute("CREATE TABLE %s(userid int,movieid int,rating numeric)"%(tablename))
			print("Created ",tablename)
		except:
			print("Table Creation Error")
		openconnection.commit()	
		i=i+1
	row_id=1
	tablecount=0;
	while tablecount<n:
		if tablecount==0:
			table_name="rrpartition"+str(int(n))
			try:
				cur.execute("Insert into %s(select userid,movieid,rating from (select row_number() over() as row_num, * from %s) AS foo where mod(row_num,%d)=%d)"%(table_name,ratingstablename,n,tablecount))
				print("Inserted into ",table_name)
			except Exception as details:
				print("table insertion error 1",details)
			openconnection.commit()
		else:
			table_name="rrpartition"+str(tablecount)
			try:
				cur.execute("Insert into %s(select userid,movieid,rating from (select row_number() over() as row_num, * from %s) AS foo where mod(row_num,%d)=%d)"%(table_name,ratingstablename,n,tablecount))
				print("Inserted into ",table_name)
			except:
				print("table insertion error 2")
			openconnection.commit()
		tablecount=tablecount+1


# Inserts into the Round Robin partition based on where the last insertion was made

def roundrobininsert(ratingstablename, userid, itemid, rating,openconnection):
	global noofpartition1
	cur=openconnection.cursor()
	cur.execute("select count(*) from ratings")
	row=cur.fetchone()
	row_count=row[0]
	row_mod=(row_count+1)%noofpartition1
	if row_mod==0:
		tablename="rrpartition"+str(int(noofpartition1))
	else:
		tablename="rrpartition"+str(int(row_mod))
	try:
		
		cur.execute("INSERT INTO %s values(%d,%d,%f)"%(ratingstablename,userid,itemid,rating))
		cur.execute("INSERT INTO %s values(%d,%d,%f)"%(tablename,userid,itemid,rating))
		print("Round Robin Insert")
		print("Inserted Into ",tablename)
	except Exception as detail:
		print("Insertion Error ",detail)
	openconnection.commit()
	

#Inserts into Range partition depending on the range that it belongs to

def rangeinsert(ratingstablename, userid, itemid, rating,openconnection):
	if rating<0 or rating >5:
		return
	#Keeping this global so that it uses the noofpartition value that was passed in the rangepartition method
	global noofpartition
	cur=openconnection.cursor()	
	print(noofpartition)
	range=5/noofpartition
	end=range
	start=0
	tablecount=1		
	while True:
		if rating>start and rating<=end:
			tablename="rangepartition"+str(tablecount)
			try:
				cur.execute("INSERT INTO %s values(%d,%d,%f)"%(tablename,userid,itemid,rating))
				print("Range Partition Insert")
				print("Inserted Into ",tablename)
			except Exception as detail:
				print("Insertion into table failed",detail)			
			break
		else:
			start=end
			end=end+range
			tablecount=tablecount+1
			continue
	openconnection.commit()

# Deletes Both the range partition tables and the Round Robin Partition Tables 

def deletepartition(con):
	cur=con.cursor()
	cur.execute("select tablename from pg_tables where tablename LIKE 'rrpartition%'")
	rrtablename=cur.fetchall()
	cur.execute("select tablename from pg_tables where tablename LIKE 'rangepartition%'")
	rangetablename=cur.fetchall()
	rrsize=len(rrtablename)-1
	#print rangetablename[0]
	try:
		while rrsize>=0:
			cur.execute("DROP TABLE %s"%(rrtablename[rrsize]))
			print("Deleted ",rrtablename[rrsize])
			rrsize=rrsize-1
			con.commit()
	except Exception as detail:
		print(detail)
	rangesize=len(rangetablename)-1
	try:
		while rangesize>=0:
			cur.execute("DROP TABLE %s"%(rangetablename[rangesize]))
			print("Deleted ",rangetablename[rangesize])
			rangesize=rangesize-1 
			con.commit()
	except Exception as detail:
		print(detail)
		

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = psycopg2.connect(user='postgres', host='localhost', password='__PASSWORD_FOR_USER_POSTGRES__')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def before_db_creation_middleware():
	global DB_NAME
	DB_NAME="dds_assgn1"


def after_db_creation_middleware(databasename):
	'''global ratingspath	
	global ratingsfilepath	
	global noofpartition	
	ratingspath=input("Please enter the 'Ratings.dat' file path in double quotes (Exclude the filename) ")
	ratingsfilepath=ratingspath+"/ratings.dat"
	noofpartition=int(input("Please enter the number of partitions "))'''
	pass

def before_test_script_starts_middleware(openconnection, databasename):
	'''global user_id
	global movie_id
	global rating
	user_id=int(input("Enter the user ID: "))
	movie_id=int(input("Enter the movie ID: "))
	rating=float(input("Enter the rating: "))'''
	pass


if __name__ == '__main__':
	try:
	# Use this function to do any set up before creating the DB, if any
		before_db_creation_middleware()

		create_db(DB_NAME)

		# Use this function to do any set up after creating the DB, if any
		after_db_creation_middleware(DB_NAME)
		
	except Exception as detail:
		print("OOPS! This is the error ==> ", detail)
	
	con=getopenconnection()

	# Uncomment these Functions calls to  Run the program
	# Rangepartition and Range insert should be run back to back
	# Roundrobin partition and Round Robin insert should be run after each other

	loadratings("ratings","/home/siddhu/test_data.dat", con)
	rangepartition("ratings",3,con)
	roundrobinpartition("ratings",5,con)
	roundrobininsert("ratings",101,3344,4,con)
	rangeinsert("ratings",102,3345,2,con)
	#deletepartition(con)
