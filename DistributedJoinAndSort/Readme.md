Siddharth Sujir Mohan
1207654769

ASSIGNMENT 3 Readme.txt
------------------------

Assumptions
------------

1) The input tables are already loaded into the database
2) The number of partitions are hardcoded to 5

Basic Steps
--------------
1) In the get connection function, change the database name, username and the password

    getopenconnection(user='postgres', password='xxxxxxx', dbname='ddb_assignment')


To Run Parellel Sort function
------------------------------
In the main method, uncomment the following funtion call and replace the necessary parameters

#parellelSort("Table","SortingColumnName","OutputTable",con)


The parellel sort function splits the table into 5 partitions and runs the sorting on those tables using mutiple threads which is created in the parellel sort function


To Run the parellel join function
-----------------------------------
In the main method, uncomment the following line and give the necessary arguments


#parellelJoin("InputTable1","InputTable2","Table1JoinColumn","Table2JoinColumn","OutputTable",con)

The parellel join function splits the table into 5 partitions and runs the join operation on those tables using mutiple threads which is created in the parellel join function
