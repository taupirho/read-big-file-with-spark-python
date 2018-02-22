# read-big-file-with-spark
We try to read the same big file (24 Gbytes) we read before with python but 
this time using Spark. It won't be a true test as we are only running this 
on my local PC not on a proper cluster. Just thought it would be interesting
to try it out.

I ran this on a Windows 7 PC with 16Gbytes of ram using python version 3.5,
pyspark 2.1 and a Jupyter notebook. I used the same "big file" as was used in my 
other repository - read-big-file-with-python.

The job took 37 minutes to complete but bear in mind there would still have to be a 
bit of post processing to be done to collect all the disparate files together. This 
compares with the 18 minutes it took to process the same file using just python 3.6 
on the same PC and the 54 minutes it took a C program to process it on an HP Alpha box
