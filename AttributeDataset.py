import pandas as pd
import  mysql.connector as conn
import  logging

logging.basicConfig(filename="AttributeDataset.log", level=logging.DEBUG,format='%(asctime)s %(levelname)s %(name)s %(message)s')

logging.info("First step is to fetch the data from xlsx using pandas library ")
df= pd.read_excel('Attribute DataSet.xlsx',index_col=False)
logging.info("Data is fetched successfully from xlsx file ")

logging.info("Convert a data from xlsx into json format")
convertedintojson=df.to_json()
print(convertedintojson)
logging.info("Data Converted into Json format successfully")

logging.info("Inser json data into mongo db ")
try:
    logging.info("Make a connection with MongoDB")
    client = pymongo.MongoClient("mongodb+srv://swanandwalke11:Altitude@cluster0.goi2j.mongodb.net/?retryWrites=true&w=majority")
    db = client.test
    logging.info("Connection is successfully established")
except Exception as e:
    logging.info("Gwtting an error ehile connection to MongoDB",e)

try:
    logging.info("Create a new database here")
    AttributeSaleData = client['inventory']
    logging.info("database  created ")

    logging.info("Create a new collection")
    collection_AttributeSale = AttributeSaleData["table"]
    logging.info("new collection  created ")

    logging.info("Insert json data into collection")
    collection_AttributeSale.insert_many(convertedintojson)
    logging.info("data inserted into collection")

except Exception as e:
    logging.info("Data not inserted successfully into a collection")



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

logging.info("Create a table AttributeDataset")
try:
    logging.info("Creating a new table AttributeDataset")
    #cursor.execute("create table sqlassignment.AttributeDataset(Dress_ID int(20), Style varchar(50), Price varchar(50), Rating FLOAT(5.5), Size varchar(50), Season varchar(50), NeckLine varchar(50), SleeveLength varchar(50), waiseline varchar(50), Material varchar(50), FabricType varchar(50), Decoration varchar(50), PatternType varchar(50), Recommendation int(10))")
    logging.info("table created")

    for i,row in df.iterrows() :
        sql = "insert into sqlassignment.AttributeDataset values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql,tuple(row))
        mydb.commit()
    logging.info("Data sucessfully inserted into AttributeDataset table")

except Exception as e:
    print("Error while insrting records to AttributeDataset table", e)

logging.info("Select all rows from AttributeDataset table")
sql = "SELECT * FROM sqlassignment.AttributeDataset"
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

logging.info("Make a left join on AttributeDataset and Dress Sales tables")
leftjoin = "select AttributeDataset.Style, DressSales.SalesDate_1 from AttributeDataset left join DressSales on AttributeDataset.Dress_ID = DressSales.Dress.ID"
logging.info("Execute the query")
cursor.execute(leftjoin)
logging.info("query Executed successfully")


logging.info("sql query to find out unique dress that we have based on dress id ")
uniquedressid = "select distinct count(Dress_ID) from sqlassignment.AttributeDataset"
logging.info("Execute the query")
cursor.execute(uniquedressid)
logging.info("query Executed successfully")


logging.info("Sql query to find out  dress is having recommendation 0")
countdressidzero = "select count(Dress_ID) from sqlassignment.AttributeDataset where AttributeDataset.Recommendation=0"
logging.info("Execute the query")
cursor.execute(countdressidzero)
logging.info("query Executed successfully")


logging.info("sql query to find out total dress sell for individual dress id ")
uniquedressid = "select  count(Dress_ID),sum(Dress_ID) from sqlassignment.AttributeDataset"
logging.info("Execute the query")
cursor.execute(uniquedressid)
logging.info("query Executed successfully")



logging.info("sql query to find out a third highest most selling dress id ")
uniquedressid = "select Dress_ID from sqlassignment.AttributeDataset order by Dress_ID desc limit 2,1"
logging.info("Execute the query")
cursor.execute(uniquedressid)
logging.info("query Executed successfully")



