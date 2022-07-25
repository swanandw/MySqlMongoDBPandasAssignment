import pandas as pd
import  mysql.connector as conn
import  logging

logging.basicConfig(filename="AttributeDataset.log", level=logging.DEBUG,format='%(asctime)s %(levelname)s %(name)s %(message)s')

logging.info("First step is to fetch the data from xlsx using pandas library ")
df= pd.read_excel('Dress Sales.xlsx',index_col=False)

logging.info("Second step is to make connection to MySQl database")
try:
    logging.info("Making a connection with MySql database")
    mydb= conn.connect(host = "localhost", user ="root", passwd ="Altitude@11" )
    print(mydb)
    cursor = mydb.cursor()
    cursor = mydb.cursor(buffered=True)

    logging.info("Connection is successful")
except Exception as e:
    print("Error while connecting to MySQL", e)

logging.info("Create a table DressSales")
try:
    logging.info("Creating a new table DressSales")
    cursor.execute("create table sqlassignment.DressSales(Dress_ID int(50),SalesDate_1 int(50),	SalesDate_2 int(50), SalesDate_3 int(50), SalesDate_4	int(50), SalesDate_5 int(50),	SalesDate_6 int(50),	SalesDate_7 int(50), 	SalesDate_8 int(50),	SalesDate_9 int(50),	SalesDate_10 int(50),	SalesDate_11 int(50),	SalesDate_12 int(50),	SalesDate_13 int(50),	SalesDate_14 int(50),	SalesDate_15 int(50),	SalesDate_16 int(50),	SalesDate_17 int(50),	SalesDate_18 int(50),	SalesDate_19 int(50),	SalesDate_20 int(50),	SalesDate_21 int(50),	SalesDate_22 int(50),	SalesDate_23 int(50))")
    logging.info("table created")

    for i,row in df.iterrows() :
        sql = "insert into sqlassignment.DressSales values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql,tuple(row))
        mydb.commit()
    logging.info("Data sucessfully inserted into DressSales table")

except Exception as e:
    print("Error while insrting records to DressSales table", e)

logging.info("Select all rows from DressSales table")
sql = "SELECT * FROM sqlassignment.DressSales"
logging.info("Execute the query")
cursor.execute(sql)
logging.info("query Executed successfully")

logging.info("selct all the rows ")
allrecords =cursor.fetchall()
logging.info("All rows being selected")

logging.info("Print all the rows")
for i in allrecords:
    print(i)
logging.info("all rows are printed")







