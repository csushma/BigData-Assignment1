README
Please use below link to get the data
https://drive.google.com/folderview?id=0B0vdRkcBHx41RDlsbkloQkp1MUE&usp=sharing
In order to execute please change the path pointing to dataset accordingly

I am using sandbox hortonworks on my virtual machine.

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
