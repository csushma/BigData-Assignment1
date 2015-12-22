README
Please use below link to get the data
https://drive.google.com/folderview?id=0B0vdRkcBHx41RDlsbkloQkp1MUE&usp=sharing
In order to execute please change the path pointing to dataset accordingly

I am using sandbox hortonworks on my virtual machine.

Queries for Mapreduce:
Q1:
Q 1 a: Count the total number of reviews,
Q 1 b: Count total number of users
Q 1 c: Count total number of business entities in the data.csv file.

Q2.
List each business Id that are located in “Palo Alto” using the full_address column as the
filter column.

Q3
Find the top ten rated businesses using the average ratings.
The star column represents the rating.

Q4:
Please use reduce side join and job chaining technique to answer question 4.

Q5: Please use Map side join technique to answer this question

Steps to execute
----------------
create input folder
copy all data files into input folder
copy all jar files into user/hue folder on sandbox hortonworks
execute following commands to run jar files
remove output folder after every execution of jar file


Commands to run jar files
------------------------------
Question1
hadoop jar q1a.jar Q1a input/review.csv output
hadoop jar q1b.jar Q1b input/business.csv output
hadoop jar q1c.jar Q1c input/user.csv output

Question2
hadoop jar q2.jar Q2 input/data.csv output

Question3
hadoop jar q3.jar Q3 input/data.csv output

Question4
hadoop jar q4.jar Q4 input/data.csv output

Question5
hadoop jar q5.jar Q5 input/review.csv output

Look here for implementation in 
PIG,Hive, Cassandra -> https://github.com/csushma/BigData-Assignment2
 and Scala -> https://github.com/csushma/BigData-Assignment3
