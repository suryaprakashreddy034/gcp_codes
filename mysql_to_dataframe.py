import mysql.connector


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Surya@8897",
  database="world"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM world.city")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)
