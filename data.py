import MySQLdb as cm
import os
mydb = cm.connect(host="localhost",user="root",port=3307 ,passwd="2312",database="hospital")

try:
    mycursor = mydb.cursor()
    
    file=open("data.txt",'r')

    #print(file.read())
   

    for i in file :
        list = i.split('|')
        print()
        if(list[1]=='H'):
            country=list[9]
            customer_id=int(list[3])
            try:
                Create_Table="""CREATE TABLE """+country+""" ( 
                                                 Customer_Name varchar(255) NOT NULL,                             
                                                 Customer_Id varchar(18) NOT NULL PRIMARY KEY,                                                                                     
                                                 Customer_Open_Date Date NOT NULL,
                                                 Last_Consulted_Date Date ,                            
                                                 Vaccination_Type char(5) ,
                                                 Doctor_Consulted char(255) ,
                                                 State char(5) ,
                                                 Country char(5) ,
                                                 Postcode int(5) ,
                                                 Date_of_Birth Date ,
                                                 Active_Customer char(1) 
                                                )"""
                mycursor = mydb.cursor()
                result = mycursor.execute(Create_Table)
                print("Customers Table created successfully:- ")
                mydb.commit()
            except:
                pass
            finally:
                mySql_insert_query = """INSERT INTO """ + country + """ (Customer_Name,Customer_Id,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted, State,Country,Postcode,Date_of_Birth,Active_Customer,Active_Customer) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                mycursor = mydb.cursor()
                result = mycursor.execute(mySql_insert_query, (
                    list[2], list[3], list[4], list[5], list[6], list[7], list[8], list[9], list[10], list[11],
                    list[12]))
                print(mycursor.rowcount, "Record inserted successfully into Country table.")
                mydb.commit()
                mycursor.close()
                mydb.close()
    print("DATA inserted in Tabels succesfully :)") 
except cm.Error as error:
            print("Failed to create table in MySQL:- {}".format(error))
            print("Failed to insert record into Customers table {}".format(error))

file.close()
