"""
Automate batch scripts
The goal is to allow users to automate the running of all sql scripts they need as part of their process
"""


# Import all packages
import psycopg2 # Allows Python to interact with SQL
import getpass # Hides password inputs
import glob # Reads filenames in a folder
import time # USed to track script run times
import sys # Only used to stop script while testing
import os # Allows for directory manipulation



# define function to loop through and run all scripts
def run_scripts():
	cur = con.cursor()

	for i in scriptlist:
		print("Starting to run " + os.path.basename(i))
		start_time = time.time()
		cur.execute(open(i, "r").read())
		end_time = time.time()
		run_time = end_time - start_time
		print(os.path.basename(i) + " - Completed. It took " + str("{0:.5f}".format(run_time)) + " seconds")
		print("")

	cur.close()
	con.close()

	print("All scripts have been run")


# Prints out the file names of the scripts to be run
def print_scriptlist():
	print("These are the sql scripts that will be run: ")
	k = 1
	for i in scriptlist:
		print(str(k) + ") " + os.path.basename(i))
		k += 1
	print("")


def endscript():
	print("Script End")
	# Redirect back to home directory
	os.chdir(r"C:\Users\hzheng\Documents\koopa")
	sys.exit()

"""
Main Body
"""


# Log into appropriate cluster
# More secure way to connect to the Performance Cluster by requesting the username and password from the user
print ("Welcome to Automated Batch Scripts")
print("Log into Performance Cluster")
# clusterUser = input('Enter your Performance cluster username: ')
# clusterPassword = getpass.getpass('Enter your Performance cluster password: ')
clusterUser = 'dwhanalyst'
clusterPassword = 'cj5piDrnAVbx48LWSgWwxfTVFQ07myOiYouwIIrT2Pd'
con = psycopg2.connect(dbname = 'earnest',
	host = 'earnest-spark-cluster.ca1y9bisuetf.us-east-1.redshift.amazonaws.com',
	port = '5439',
	user = clusterUser,
	password = clusterPassword)
con.autocommit = True
print("")

# Save all sql file to be automated into a list
# Asks user for folder location of scripts
# Makes that folder the active directory
# foldername = input("Which folder are your sql scripts stored in? (paste in full folder path) ")
foldername = r"C:\Users\hzheng\Documents\koopa\QC"
print("")
os.chdir(foldername)
filetype = ".sql"
scriptlist = glob.glob(foldername + "/*" + filetype)

print_scriptlist()
# endscript()

# Checks if the scripts are the ones the user wants to run
scriptcheck = input("Are these the correct SQL scripts you want to run? (y/n) ")

if scriptcheck == "y":
	run_scripts()
	endscript()
else:
	# endscript()
	# global ifedit
	ifedit = input("Do you want to remove scripts from the list? (y/n) ")
	if ifedit == 'n':
		endscript()
	else:
		while ifedit == 'y':
			print_scriptlist()
			filepos = int(input("Which file do you want to remove from this list? (#) "))
			del scriptlist[filepos-1]
			print("This an updated list of scripts to be run: ")
			print_scriptlist()
			ifedit = input("Do you want to remove another script from the list? (y/n) ")
			if ifedit == 'n':
				run_scripts()
				endscript()