import MySQLdb as cm
import os
mydb = cm.connect(host="localhost",user="root",port=3307 ,passwd="2312",database="hospital")
try:
    mycursor = mydb.cursor()
    flag=0
    Country=0
    
    file=open("data.txt",'r')
    
    for i in file:
        list = i.split('|') #Split data from file using | Symbol
        Country=(list[1])
        print(Country)
        if(list[1]=='D'):

            name=list[2]
            start=list[3]
            flag = 0
            showQuery = "show tables;" #Find tables in Database
            mycursor.execute(showQuery) #Execute the Query
            result = mycursor.fetchall() # Fetch all data from the database
            for i in result:
                if (i[0].upper() == Country):
                    mySql_insert_query = """INSERT INTO """+Country+""" (Customer_Name,Customer_Id,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted, State,Country,Postcode,Date_of_Birth,Active_Customer) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                    mycursor = mydb.cursor()
                    result = mycursor.execute(mySql_insert_query, (
                        list[2], list[3], list[4], list[5], list[6], list[7], list[8], list[9], list[10], list[11],
                        list[12]))
                    print(mycursor.rowcount, "Record inserted successfully into Country table.")

                    flag = 1
                    break
            if (flag == 0):
                mySql_Create_Table_Query = """CREATE TABLE """+Country+""" ( 
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
                                                ) """
                print()
                mycursor = mydb.cursor()
                result = mycursor.execute(mySql_Create_Table_Query)
                print("Country Table created successfully:- ")
                mySql_insert_query = """INSERT INTO """ + Country + """ (Customer_Name,Customer_Id,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted, State,Country,Postcode,Date_of_Birth,Active_Customer) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                mycursor = mydb.cursor()
                result = mycursor.execute(mySql_insert_query, (
                    list[2], list[3], list[4], list[5], list[6], list[7], list[8], list[9], list[10], list[11],
                    list[12]))
                print(mycursor.rowcount, "Record inserted successfully into Country table.")

            else:
                #                       mySql_Create_Table_Query = """CREATE TABLE Customers (
                #                             Customer_Name varchar(255) NOT NULL,
                #                             Customer_Id varchar(18) NOT NULL PRIMARY KEY,
                #                             Customer_Open_Date Date NOT NULL,
                #                             Last_Consulted_Date Date ,
                #                             Vaccination_Type char(5) ,
                #                             Doctor_Consulted char(255) ,
                #                             State char(5) ,
                #                             Country char(5) ,
                #                             Postcode int(5) ,
                #                             Date_of_Birth Date ,
                #                           ) """
                mycursor = mydb.cursor()
                result = mycursor.execute(mySql_Create_Table_Query)
                print("Customers Table created successfully:- ")
                mySql_insert_query = """INSERT INTO """ + Country + """ (Customer_Name,Customer_Id,Customer_Open_Date,Last_Consulted_Date,Vaccination_Type,Doctor_Consulted, State,Country,Postcode,Date_of_Birth,Active_Customer) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                mycursor = mydb.cursor()
                result = mycursor.execute(mySql_insert_query, (
                    list[2], list[3], list[4], list[5], list[6], list[7], list[8], list[9], list[10], list[11],
                    list[12]))
                print(mycursor.rowcount, "Record inserted successfully into Customers table.")


except cm.Error as error:
            print("Failed to create table in MySQL:- {}".format(error))
            print("Failed to insert record into Customers table {}".format(error))
'''finally:
            mydb.commit()
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed.")'''
file.close()
