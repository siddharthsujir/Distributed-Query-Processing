The pathname of the dataset file has been hardcoded in the script. Also, the rating value input for Point query and Min and Max rating values input for Range query have also been hardcoded in the script.

In order to run the python script, the procedure is as below:
python Interface_Assignment2.py
1) New database is created, if it already does not exist.
2) Existing partitions are deleted, and new partitions are created using either Round robin or Range method (Both options are available in the script, however call for one of them is commented out for a given run)
3) Range query function is called, the function goes to each partition and retrieves all those tuples in the given range and inserts those tuples into a text file called hello.txt.
4) Point query function is called, the function goes to each partition and retrieves all those tuples that satisfy the condition and inserts those tuples into a text file called hello_pointquery.txt.

----------------------------
Team13 Details:
----------------------------
Rakesh Subramanian Suresh 1207412319
Siddharth Mohan 	  1207654769
Arpan Chatterjee	  1207494817
Kranthi Sai Davuluri 	  1208677310
Sagar Kalburgi		  1207690402