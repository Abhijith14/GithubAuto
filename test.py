import psycopg2
from github import Github
from pathlib import Path
from datetime import date
from time import sleep
import pandas as pd


df1 = pd.read_csv("status.csv", delimiter=',')
ID_Stat = df1['ID'].values.tolist()
Calendar = df1['Date'].values.tolist()
# Name_Stat = df1['Name'].values.tolist()

ID_Stat = (-1, 0) + tuple(ID_Stat)

print(ID_Stat)

def mergeCode(Qno, Qname, Qdesc, Session, Code):
    Qname = Qname.replace(" ", "")
    Session = Session.replace(" ", "")
    Qdesc = Qdesc.replace("<br>", "\n")
    parseAns = "//" + Qno + '\n' \
               "//" + Qname + "(" + Session + ")" + '\n'\
               "/*\n" + Qdesc + "\n*/\n\n" + Code
    return parseAns


def ConnectGB(Code, Name):
    g = Github(ACCESS_TOKEN)

    repo = g.get_repo("Abhijith14/JavaElab")

    contents = repo.get_contents("")

    i = 0
    Name = Name.replace(" ", "")
    destination = Name + ".java"
    Names = []

    while contents:
        file_content = contents.pop(0)
        Names.append(file_content.name)

    restart = True

    while restart:
        for name_val in range(len(Names)):
            restart = False
            if Names[name_val] == destination:
                i = i + 1
                destination = Name + "(" + str(i) + ").java"
                restart = True
                break

    repo.create_file(destination, "Commit", Code, branch="main")


def ConnectDB():
    mydb = psycopg2.connect(
        host="HOST",
        database="DATABASE",
        user="USER",
        password="PASSWORD"
    )

    mycursor = mydb.cursor()

    print("CONNECTED TO DB")
    print("----------------------------------------------------------")

    t = tuple(ID_Stat)

    SQL = "SELECT * FROM elabdata WHERE ID NOT IN {} ORDER BY ID ASC".format(t)

    mycursor.execute(SQL)

    ans = mycursor.fetchall()
    count = ID_Stat[-1]

    for row in ans:

        today = date.today()

        print()
        print("******************************")
        print(str(today))
        print(str(Calendar))
        print("******************************")
        print()

        while str(today) in str(Calendar):
            today = date.today()
            print()
            print("Waiting for Next Day ...")
            print("Date Stored : ")
            print(Calendar)
            print(str(count) + " Questions out of " + str(len(ans)) + " (" + str(len(ans) - count) + " remaining).")
            print()
            sleep(5)

        Calendar.append(str(today))

        count = count + 1
        Code = row[0]
        QDesc = row[1]
        Qname = row[2]
        QNo = row[3]
        Session = row[4]
        ID = row[5]

        parsed_Code = mergeCode(QNo, Qname, QDesc, Session, Code)

        print("Typing Question - " + str(Qname.replace(" ", "")) + " --------- #" + str(ID) + " -------> " + str(
            count) + "/" + str(len(ans)) + " (" + str(len(ans) - count) + " remaining).")

        # ConnectGB(parsed_Code, Qname)
        Create_local_File(parsed_Code, Qname)

        new_row = [{'ID': ID, 'Date': str(today), 'Name': Qname.replace(" ", "")}]
        new_data = df1.append(new_row)
        new_data.to_csv("status.csv", index=False)

        print("Finished!!!")
        sleep(5)


def Create_local_File(Data, Name):
    i = 0
    Name = Name.replace(" ", "")
    destination = "Java/" + Name + ".java"

    while Path(destination).is_file():
        i = i + 1
        destination = "Java/" + Name + "(" + str(i) + ").java"

    f = open(destination, "w+")
    f.write(Data)
    f.close()

    # return destination


ConnectDB()
