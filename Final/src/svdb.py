import socket
import mysql.connector
import socket

import time

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverSocket.bind(("0.0.0.0",9000))
serverSocket.listen()

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mysqlpw",
    port="49153",
    database="strokedata"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM stroke")

myresult = mycursor.fetchall()

while(True):    
    (clientConnected, clientAddress) = serverSocket.accept()

    print("Accepted a connection request from %s:%s"%(clientAddress[0], clientAddress[1]))

    for x in myresult:
        a = ",".join([str(e) for e in x])
        s =  a.strip() + "\n"
        print (s)
        clientConnected.send((s).encode())
        time.sleep(0.1)
