import pandas as pd
import psycopg2
import re
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import csv

default_args={
    'start_date' : datetime(2024, 4, 29)
}



connection = psycopg2.connect(
   dbname = 'finpro1',
   user = 'postgres',
   password = 'nadya123',
   host = 'host.docker.internal',
   port = '5432'
)

cursor = connection.cursor()

#cursor.execute('select * from drugs')
#rows = cursor.fetchall()
#for row in rows:
#    print(row)
'''
hospital
doctor
drugs
lab
patient
room
surgery
'''



# def create_table_hospital(**kwargs):
#     drop_query = '''drop table if exists hospital'''
#     cursor.execute(drop_query)
#     connection.commit()

#     query = '''
#             create table if not exists hospital (
#             	id	int primary key,
#             	date_in date,
#             	date_out date,
#             	branch varchar,
#             	hospital_care varchar,
#             	drug_quantity int,
#             	admin int,
#             	cogs int,
#             	payment varchar,
#             	review varchar,
#             	patient_id int,
#             	room_id int,
#             	drug_id int,
#             	doctor_id int,
#             	surgery_id int,
#             	lab_id int
#             )
#             '''
#     cursor.execute(query)
#     connection.commit()

#     df = pd.read_csv(r"/opt/airflow/csv/hospital_new.csv").fillna(0)
#     df.rename(columns = {'ID': 'id', 'Date IN': 'date_in', 'Date OUT': 'date_out', 'Branch': 'branch', 'Hospital Care': 'hospital_care', 'Drug Qty':'drug_quantity', 'Drug ': 'drug', 'Admin': 'admin', 'COGS': 'cogs', 'Payment': 'payment', 'Review': 'review'}, inplace = True)


#     column_list = df.columns.tolist()
    
#     dum_list = []
#     for i in range(len(df)):
#         values_tuple = tuple(df.loc[i].values)
#         if i != len(df) - 1:
#            dum_list.append(str(values_tuple))
#         else:
#            dum_list.append(str(values_tuple))
    
    
#     sql_query = f"INSERT INTO hospital ({', '.join(column_list)}) VALUES\n"
    
#     for j in range(len(dum_list)):
#         if j != len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}, \n"
#         elif j == len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}"
    
#     cursor.execute(sql_query)
#     connection.commit()
    

# def create_table_doctor(**kwargs):
#     drop_query = '''drop table if exists doctor'''
#     cursor.execute(drop_query)
#     connection.commit()

#     query='''
#           create table if not exists doctor (
# 			doctor_id int primary key,
# 			doctor varchar,
# 			doctor_price int
#           )
#           '''
    
#     cursor.execute(query)
#     connection.commit()

#     df = pd.read_csv(r"/opt/airflow/csv/doctor.csv").fillna(0)
#     df.rename(columns = {'Doctor':'doctor', 'Doctor Price' : 'doctor_price'}, inplace = True)


#     column_list = df.columns.tolist()
    
#     dum_list = []
#     for i in range(len(df)):
#         values_tuple = tuple(df.loc[i].values)
#         if i != len(df) - 1:
#            dum_list.append(str(values_tuple))
#         else:
#            dum_list.append(str(values_tuple))
    
    
#     sql_query = f"INSERT INTO doctor ({', '.join(column_list)}) VALUES\n"
    
#     for j in range(len(dum_list)):
#         if j != len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}, \n"
#         elif j == len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}"
    
#     cursor.execute(sql_query)
    
#     connection.commit()


# def create_table_drugs(**kwargs):
#     drop_query = '''drop table if exists drugs'''
#     cursor.execute(drop_query)
#     connection.commit()


#     query = '''
#             create table if not exists drugs(
# 			    drug_id int primary key,
# 			    drug_brand varchar,
# 			    drug_type varchar,
# 			    drug_price int
#             )
#             '''
#     cursor.execute(query)
#     connection.commit()

#     df = pd.read_csv(r"/opt/airflow/csv/drugs.csv").fillna(0)
#     df.rename(columns = {'Drug Brands':'drug_brand', 'Drug Types' : 'drug_type', 'Drug Price per Item': 'drug_price'}, inplace = True)


#     column_list = df.columns.tolist()
    
#     dum_list = []
#     for i in range(len(df)):
#         values_tuple = tuple(df.loc[i].values)
#         if i != len(df) - 1:
#            dum_list.append(str(values_tuple))
#         else:
#            dum_list.append(str(values_tuple))
    
    
#     sql_query = f"INSERT INTO drugs ({', '.join(column_list)}) VALUES\n"
    
#     for j in range(len(dum_list)):
#         if j != len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}, \n"
#         elif j == len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}"
    
#     cursor.execute(sql_query)
    
#     connection.commit()

# def create_table_lab(**kwargs):
#     drop_table = '''drop table if exists lab'''
#     cursor.execute(drop_table)
#     connection.commit()
    
#     query = '''
#             create table if not exists lab (
# 			    lab_id int primary key,
# 			    lab varchar,
# 			    lab_price int
#             )
#             '''
#     cursor.execute(query)
#     connection.commit()

#     df = pd.read_csv(r"/opt/airflow/csv/lab.csv").fillna(0)
#     df.rename(columns = {'Lab':'lab', 'Lab Price': 'lab_price'}, inplace = True)


#     column_list = df.columns.tolist()
    
#     dum_list = []
#     for i in range(len(df)):
#         values_tuple = tuple(df.loc[i].values)
#         if i != len(df) - 1:
#            dum_list.append(str(values_tuple))
#         else:
#            dum_list.append(str(values_tuple))
    
    
#     sql_query = f"INSERT INTO lab ({', '.join(column_list)}) VALUES\n"
    
#     for j in range(len(dum_list)):
#         if j != len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}, \n"
#         elif j == len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}"
    
#     cursor.execute(sql_query)
    
#     connection.commit()


# def create_table_patient(**kwargs):
#     drop_query = '''drop table if exists patient'''
#     cursor.execute(drop_query)
#     connection.commit()
#     query = """
#             create table if not exists patient(
#         		patient_id int primary key,
#         		patient_name varchar(255),
#         		gender varchar(255),
#         		age int
#             )
#             """
    
#     cursor.execute(query)
#     connection.commit()

#     df = pd.read_csv(r"/opt/airflow/csv/patient_new.csv", sep=";").fillna(0)
#     df.rename(columns = {'Name':'patient_name', 'Age': 'age', 'Gender': 'gender'}, inplace = True)


#     column_list = df.columns.tolist()
    
#     dum_list = []
#     for i in range(len(df)):
#         values_tuple = tuple(df.loc[i].values)
#         if i != len(df) - 1:
#            dum_list.append(str(values_tuple))
#         else:
#            dum_list.append(str(values_tuple))
    
    
#     sql_query = f"INSERT INTO patient ({', '.join(column_list)}) VALUES\n"
    
#     for j in range(len(dum_list)):
#         if j != len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}, \n"
#         elif j == len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}"
    
#     cursor.execute(sql_query)
    
#     connection.commit()

# def create_table_room(**kwargs):
#     drop_query = '''drop table if exists room'''
#     cursor.execute(drop_query)
#     connection.commit()


#     query = """
#             create table if not exists room (
# 			    room_id int primary key,
# 			    room varchar,
# 			    food_price int,
# 			    room_price int
#             )
#             """
    
#     cursor.execute(query)
#     connection.commit()

#     df = pd.read_csv(r"/opt/airflow/csv/room_new.csv").fillna(0)
#     df.rename(columns = {'Room':'room', 'Food': 'food_price', 'Room Price': 'room_price' }, inplace = True)


#     column_list = df.columns.tolist()
    
#     dum_list = []
#     for i in range(len(df)):
#         values_tuple = tuple(df.loc[i].values)
#         if i != len(df) - 1:
#            dum_list.append(str(values_tuple))
#         else:
#            dum_list.append(str(values_tuple))
    
    
#     sql_query = f"INSERT INTO room ({', '.join(column_list)}) VALUES\n"
    
#     for j in range(len(dum_list)):
#         if j != len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}, \n"
#         elif j == len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}"
    
#     cursor.execute(sql_query)
    
#     connection.commit()

# def create_table_surgery(**kwargs):
#     drop_query = '''drop table if exists surgery'''
#     cursor.execute(drop_query)
#     connection.commit()


#     query = """
#             create table if not exists surgery (
# 			     surgery_id int primary key,
# 			     surgery varchar,
# 			     surgery_price int
#             )
#             """
#     cursor.execute(query)
#     connection.commit()

#     df = pd.read_csv(r"/opt/airflow/csv/surgery.csv").fillna(0)
#     df.rename(columns = {'Surgery':'surgery', 'Surgery Price': 'surgery_price'}, inplace = True)


#     column_list = df.columns.tolist()
    
#     dum_list = []
#     for i in range(len(df)):
#         values_tuple = tuple(df.loc[i].values)
#         if i != len(df) - 1:
#            dum_list.append(str(values_tuple))
#         else:
#            dum_list.append(str(values_tuple))
    
    
#     sql_query = f"INSERT INTO surgery ({', '.join(column_list)}) VALUES\n"
    
#     for j in range(len(dum_list)):
#         if j != len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}, \n"
#         elif j == len(dum_list) - 1:
#            sql_query += f"    {dum_list[j]}"
    
#     cursor.execute(sql_query)
    
#     connection.commit()




def hospital():
    cursor = connection.cursor()
    cursor.execute('select * from hospital')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()

    df_hospital = pd.DataFrame(result, columns = ["id", "date_in", "date_out", "branch", "hospital_care", "drug_quantity", "admin_price", "cogs", "payment"
                                         , "review", "patient_id", "room_id", "drug_id", "doctor_id", "surgery_id", "lab_id"])
    
    return df_hospital

def patient():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from patient
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()

    df_patient = pd.DataFrame(result, columns=['patient_id', 'patient_name', 'gender', 'age'])

    return df_patient

def doctor():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from doctor
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()

    df_doctor = pd.DataFrame(result, columns = ['doctor_id', 'doctor', 'doctor_price'])

    return df_doctor

def drugs():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from drugs
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()

    df_drugs = pd.DataFrame(result, columns = ['drug_id', 'drug_brand', 'drug_type', 'drug_price'])

    return df_drugs

def lab():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from lab
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()
    df_lab = pd.DataFrame(result, columns = ['lab_id', 'lab', 'lab_price'])

    return df_lab

def room():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from room
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()
    df_room = pd.DataFrame(result, columns = ['room_id', 'room_type', 'food_price', 'room_price'])

    return df_room

def surgery():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from surgery
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()
    df_surgery = pd.DataFrame(result, columns = ['surgery_id', 'surgery', 'surgery_price'])

    return df_surgery

#df_hospital = hospital()
#df_patient = patient()
#df_surgery = surgery()
#df_doctor = doctor()
#df_drugs = drugs()
#df_room = room()
#df_lab = lab()

#print(df_hospital.head())
#print('-'*20)
#print(df_patient.head())
#print('-'*20)
#print(df_surgery.head())
#print('-'*20)
#print(df_doctor.head())
#print('-'*20)
#print(df_drugs.head())
#print('-'*20)
#print(df_room.head())
#print('-'*20)
#print(df_lab.head())


def merge_hospital(**kwargs):
    df_hospital = hospital()
    df_patient = patient()
    df_surgery = surgery()
    df_doctor = doctor()
    df_drugs = drugs()
    df_room = room()
    df_lab = lab()
    df_hospital = df_hospital.merge(df_patient, how = 'left', on = ['patient_id'])
    df_hospital = df_hospital.merge(df_room, how = 'left', on = 'room_id')
    df_hospital = df_hospital.merge(df_drugs, how = 'left', on = 'drug_id')
    df_hospital = df_hospital.merge(df_doctor, how = 'left', on = 'doctor_id')
    df_hospital = df_hospital.merge(df_surgery, how = 'left', on = 'surgery_id')
    df_hospital = df_hospital.merge(df_lab, how = 'left', on = 'lab_id')
    return df_hospital



def drugs_price(**kwargs):
    df_hospital = merge_hospital()
    drug_price_list = []
    #df_hospital['drug_price'] = df_hospital['drug_price'].str.replace(r'(Rp|.00|\,|$|\$)', '', regex=True).astype(int)
    # df_hospital['drug_price'] = df_hospital['drug_price'].str.replace('Rp', '').str.replace(',','').str.replace('.00','').str.replace('$', '').astype(int)
    for i in range(len(df_hospital)):
        drug_price = df_hospital.loc[i,'drug_quantity'] * df_hospital.loc[i,'drug_price']
        drug_price_list.append(drug_price)
    return drug_price_list


#print(df_hospital[['drug_brand', 'drug_quantity', 'drug_price', 'drug_total_price']])


def days_diff(**kwargs):
    df_hospital = merge_hospital()
    days_list = []
    for i in range(len(df_hospital)):
        if df_hospital.loc[i, 'hospital_care'] == 'Rawat Inap':
            days = (((pd.to_datetime(df_hospital.loc[i, 'date_out']) - pd.to_datetime(df_hospital.loc[i, 'date_in'])).total_seconds()) / (3600 * 24)) + 1
            days_list.append(days)
        elif df_hospital.loc[i, 'hospital_care'] == 'Rawat Jalan':
            days_list.append(0)
    return days_list


def doctor_visit(**kwargs):
    df_hospital = merge_hospital()
    df_hospital['days_diff'] = days_diff()
    doctor_visit_list = []
    #df_hospital['doctor_price'] = df_hospital['doctor_price'].str.replace(r'(Rp|.00|\,|$|\$)', '', regex = True).astype(int)
    # df_hospital['doctor_price'] = df_hospital['doctor_price'].str.replace('Rp', '').str.replace(',', '').str.replace('.00', '').str.replace('$', '').astype(int)
    for i in range(len(df_hospital)):
        if (df_hospital.loc[i, 'hospital_care'] == 'Rawat Inap'):
            if df_hospital.loc[i, 'doctor'] == 'Bedah':
               doctor_price = df_hospital.loc[i, 'days_diff'] * df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df_hospital.loc[i, 'doctor'] == 'Gigi':
               doctor_price = df_hospital.loc[i, 'days_diff'] * df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df_hospital.loc[i, 'doctor'] == 'Penyakit Dalam':
               doctor_price = df_hospital.loc[i, 'days_diff'] * df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df_hospital.loc[i, 'doctor'] == 'Kandungan':
               doctor_price = df_hospital.loc[i, 'days_diff'] * df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df_hospital.loc[i, 'doctor'] == 'Umum':
               doctor_price = df_hospital.loc[i, 'days_diff'] * df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
        elif (df_hospital.loc[i, 'hospital_care'] == 'Rawat Jalan'):
            if df_hospital.loc[i, 'doctor'] == 'Bedah':
               doctor_price = df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df_hospital.loc[i, 'doctor'] == 'Gigi':
               doctor_price = df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df_hospital.loc[i, 'doctor'] == 'Penyakit Dalam':
               doctor_price = df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df_hospital.loc[i, 'doctor'] == 'Kandungan':
               doctor_price = df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df_hospital.loc[i, 'doctor'] == 'Umum':
               doctor_price = df_hospital.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
    return doctor_visit_list

def room_price_total(**kwargs):
    df_hospital = merge_hospital()
    df_hospital['days_diff'] = days_diff()
    room_price_list = []
    food_price_list = []
    # for j in range(len(df_hospital)):
    #     if df_hospital['room_type'].notnull()[j]:
    #        df_hospital.loc[j, 'room_price'] = df_hospital.loc[j, 'room_price'].replace('Rp', '').replace(',','').replace('.00','').replace('$', '')
    #        df_hospital.loc[j, 'food_price'] = df_hospital.loc[j, 'food_price'].replace('Rp', '').replace(',','').replace('.00','').replace('$', '')
           

    for i in range(len(df_hospital)):
        if df_hospital.loc[i, 'room_type'] == 'VIP':
           room_price = int(df_hospital.loc[i, 'days_diff']) * int(df_hospital.loc[i,'room_price'])
           room_price_list.append(room_price)
        elif df_hospital.loc[i, 'room_type'] == 'Kelas 1':
           room_price = int(df_hospital.loc[i, 'days_diff']) * int(df_hospital.loc[i,'room_price'])
           room_price_list.append(room_price)
        elif df_hospital.loc[i, 'room_type'] == 'Kelas 2':
           room_price = int(df_hospital.loc[i, 'days_diff']) * int(df_hospital.loc[i,'room_price'])
           room_price_list.append(room_price)
        elif df_hospital.loc[i, 'room_type'] == 'Kelas 3':
           room_price = int(df_hospital.loc[i, 'days_diff']) * int(df_hospital.loc[i,'room_price'])
           room_price_list.append(room_price)
        else:
           room_price = 0
           room_price_list.append(room_price)
    for i in range(len(df_hospital)):
        if df_hospital.loc[i, 'room_type'] == 'VIP':
           food_price = (df_hospital.loc[i, 'days_diff']) * int(df_hospital.loc[i,'food_price'])
           food_price_list.append(food_price)
        elif df_hospital.loc[i, 'room_type'] == 'Kelas 1':
           food_price = (df_hospital.loc[i, 'days_diff']) * int(df_hospital.loc[i,'food_price'])
           food_price_list.append(food_price)
        elif df_hospital.loc[i, 'room_type'] == 'Kelas 2':
           food_price = (df_hospital.loc[i, 'days_diff']) * int(df_hospital.loc[i,'food_price'])
           food_price_list.append(food_price)
        elif df_hospital.loc[i, 'room_type'] == 'Kelas 3':
           food_price = (df_hospital.loc[i, 'days_diff']) * int(df_hospital.loc[i,'food_price'])
           food_price_list.append(food_price)
        else:
           food_price = 0
           food_price_list.append(food_price)
    
    return room_price_list, food_price_list

def surgery_price_total(**kwargs):
    df_hospital = merge_hospital()
    surgery_price_list = []
    # for j in range(len(df_hospital)):
    #     if df_hospital['surgery'].notnull()[j]:
    #        df_hospital.loc[j, 'surgery_price'] = df_hospital.loc[j, 'surgery_price'].replace('Rp', '').replace(',','').replace('.00','').replace('$', '')        
    
    
    for i in range(len(df_hospital)):
        if df_hospital.loc[i,'surgery'] == 'Kecil':
           surgery_price = int(df_hospital.loc[i, 'surgery_price'])
           surgery_price_list.append(surgery_price)
        elif df_hospital.loc[i,'surgery'] == 'Besar':
           surgery_price = int(df_hospital.loc[i, 'surgery_price'])
           surgery_price_list.append(surgery_price)
        elif df_hospital.loc[i,'surgery'] == 'Kusus':
           surgery_price = int(df_hospital.loc[i, 'surgery_price'])
           surgery_price_list.append(surgery_price)
        else:
           surgery_price = 0
           surgery_price_list.append(surgery_price)
    
    return surgery_price_list



def lab_price_total(**kwargs):
    df_hospital = merge_hospital()
    lab_price_list = []
    # for j in range(len(df_hospital)):
    #     if df_hospital['lab'].notnull()[j]:
    #        df_hospital.loc[j, 'lab_price'] = df_hospital.loc[j, 'lab_price'].replace('Rp', '').replace(',','').replace('.00','').replace('$', '')
    
    for i in range(len(df_hospital)):
        if df_hospital.loc[i,'lab'] == 'Hematologi':
           lab_price = int(df_hospital.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        elif df_hospital.loc[i,'lab'] == 'Kimia Darah':
           lab_price = int(df_hospital.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        elif df_hospital.loc[i,'lab'] == 'Rontgen':
           lab_price = int(df_hospital.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        elif df_hospital.loc[i,'lab'] == 'Serologi':
           lab_price = int(df_hospital.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        elif df_hospital.loc[i,'lab'] == 'Urinalisa':
           lab_price = int(df_hospital.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        else:
           lab_price = 0
           lab_price_list.append(lab_price)
    
    return lab_price_list    

def infus_price_total(**kwargs):
    df_hospital = merge_hospital()
    df_hospital['days_diff'] = days_diff()
    infus_price_list = []
    for i in range(len(df_hospital)):
        if df_hospital.loc[i, 'hospital_care'] == 'Rawat Inap':
           infus_price = df_hospital.loc[i, 'days_diff'] * 165000
           infus_price_list.append(infus_price)
        
        else:
           infus_price = 0
           infus_price_list.append(infus_price)

    return infus_price_list

def total_price_all(**kwargs):
    df_hospital = merge_hospital()
    df_hospital['drug_price_total'] = drugs_price()
    df_hospital['days_diff'] = days_diff()
    df_hospital['doctor_visit_price'] = doctor_visit()
    df_hospital['room_price_total'], df_hospital['food_price_total'] = room_price_total()
    df_hospital['surgery_price_total'] = surgery_price_total()
    df_hospital['lab_price_total'] =  lab_price_total()
    df_hospital['infus_price_total'] = infus_price_total()
    # df_hospital['admin_price'] = df_hospital['admin_price'].str.replace(r'(Rp|.00|\,|$|\$)','', regex = True).astype(int)
    total_amount_list = df_hospital['drug_price_total'].astype(int) + df_hospital['doctor_visit_price'].astype(int) + df_hospital['room_price_total'].astype(int) + df_hospital['food_price_total'].astype(int) + df_hospital['surgery_price_total'].astype(int) + df_hospital['lab_price_total'].astype(int) + df_hospital['infus_price_total'].astype(int) + df_hospital['admin_price'].astype(int)
    total_amount_list = total_amount_list.tolist()
    return total_amount_list

def total_revenue(**kwargs):
    df_hospital = merge_hospital()
    df_hospital['total_amount'] = total_price_all()
    
    # df_hospital['cogs'] = df_hospital['cogs'].replace(r'[^0-9]', '', regex=True).astype(int)
    # df_hospital['total_amount'] = df_hospital['total_amount'].replace(r'[^0-9]', '', regex=True).astype(int)

    revenue_list = df_hospital['total_amount'] - df_hospital['cogs']
    
    revenue_list = revenue_list.tolist()
    return revenue_list

def is_DBD(**kwargs):
    df_hospital = merge_hospital()
    is_DBD_list = [False] * len(df_hospital)
    for i in range(0,len(df_hospital)):
        if df_hospital['doctor'][i] == 'Umum' or df_hospital['doctor'][i] == 'Penyakit Dalam':
            if pd.isna(df_hospital['surgery'][i]):
                if df_hospital['lab'][i]=='Hematologi'or df_hospital['lab'][i]=='Serologi' or df_hospital['lab'][i]=='Kimia Darah':
                    if df_hospital['drug_type'][i]=='Umum' or df_hospital['drug_type'][i]=='Pereda Nyeri' or df_hospital['drug_type'][i]=='Vitamin':
                        is_DBD_list[i]=True
    return is_DBD_list


def time_created(**kwargs):
    df_hospital = merge_hospital()
    time_list = []
    while len(time_list) < len(df_hospital):
          time_list.append(datetime.now())
    return time_list

def save_local_csv(**kwargs):
    df_hospital = execute()
    return df_hospital.to_csv("/opt/airflow/csv/hasil_dummy.csv", index = False)
        


def execute(**kwargs):
    df_hospital = merge_hospital()
    df_hospital['drug_price_total'] = drugs_price()
    df_hospital['days_diff'] = days_diff()
    df_hospital['doctor_visit_price'] = doctor_visit()
    df_hospital['room_price_total'], df_hospital['food_price_total'] = room_price_total()
    df_hospital['surgery_price_total'] = surgery_price_total()
    df_hospital['lab_price_total'] =  lab_price_total()
    df_hospital['infus_price_total'] = infus_price_total()
    df_hospital['total_amount'] = total_price_all()
    df_hospital['revenue'] = total_revenue()
    df_hospital['created_at'] = time_created()
    df_hospital['is_DBD'] = is_DBD()

    return df_hospital


'''
hospital
doctor
drugs
lab
patient
room
surgery
'''



with DAG('finalproject_local', 
         schedule_interval = '0 6 * * *',
         default_args = default_args, 
         catchup = False,) as dag:
    
    read_table_hospital = PythonOperator(
        task_id = 'read_hospital_table',
        python_callable = hospital,
        provide_context = True,
    )

    read_table_doctor = PythonOperator(
        task_id = 'read_doctor_table',
        python_callable = doctor,
        provide_context = True,
    )

    read_table_drugs = PythonOperator(
        task_id = 'read_drugs_table',
        python_callable = drugs,
        provide_context = True,
    )

    read_table_lab = PythonOperator(
        task_id = 'read_lab_table',
        python_callable = lab,
        provide_context = True,
    )

    read_table_patient = PythonOperator(
        task_id = 'read_patient_table',
        python_callable = patient,
        provide_context = True,
    )

    read_table_room = PythonOperator(
        task_id = 'read_room_table',
        python_callable = room,
        provide_context = True,
    )

    read_table_surgery = PythonOperator(
        task_id = 'read_surgery',
        python_callable = surgery,
        provide_context = True,
    )

    merge_df = PythonOperator(
        task_id = 'merging_all_table',
        python_callable = merge_hospital,
        provide_context = True,
    )

    calculate_drugs_price = PythonOperator(
        task_id = 'calculate_drug_price',
        python_callable = drugs_price,
        provide_context = True,
    )

    calculate_days_different = PythonOperator(
        task_id = 'calculate_day_diff',
        python_callable = days_diff,
        provide_context = True,
    )

    calculate_doctor_visit = PythonOperator(
        task_id = 'calculate_doctor_visits',
        python_callable = doctor_visit,
        provide_context = True,
    )


    calculate_room_price = PythonOperator(
        task_id = 'calculate_room_price',
        python_callable = room_price_total,
        provide_context = True,
    )

    calculate_surgery_price = PythonOperator(
        task_id = 'calculate_surgery_price',
        python_callable = surgery_price_total,
        provide_context = True,
    )

    calculate_lab_price = PythonOperator(
        task_id = 'calculate_lab_price',
        python_callable = lab_price_total,
        provide_context = True,
    )

    calculate_infus_price = PythonOperator(
        task_id = 'calculate_infus_price',
        python_callable = infus_price_total,
        provide_context = True,
    )

    calculate_total_price = PythonOperator(
        task_id = 'calculate_total_price',
        python_callable = total_price_all,
        provide_context = True,
    )


    calculate_revenue = PythonOperator(
        task_id = 'calculate_revenue',
        python_callable = total_revenue,
        provide_context = True,
    )

    add_is_DBD = PythonOperator(
        task_id = 'add_is_DBD',
        python_callable = is_DBD,
        provide_context = True,
    )

    create_time_created = PythonOperator(
        task_id = 'create_time_created',
        python_callable = time_created,
        provide_context = True,
    )

    execution = PythonOperator(
        task_id = 'execute_all_function',
        python_callable = execute,
        provide_context = True,
    )

    saving_df = PythonOperator(
        task_id = 'saving_df',
        python_callable = save_local_csv,
        provide_context = True,
    )

'''
hospital
doctor
drugs
lab
patient
room
surgery
'''

# table_hospital >> table_doctor >> table_drugs >> table_lab >> table_patient >> table_room >> table_surgery >> read_table_hospital >> read_table_doctor >> read_table_drugs >> read_table_lab >> read_table_patient >> read_table_room >> read_table_surgery >> merge_df >> calculate_drugs_price >> calculate_days_different >> calculate_doctor_visit >> calculate_room_price >> calculate_surgery_price >> calculate_lab_price >> calculate_infus_price >> calculate_total_price >> calculate_revenue >> create_time_created >> execution >> saving_df
[read_table_hospital,
read_table_doctor,
read_table_drugs,
read_table_lab,
read_table_patient,
read_table_room,
read_table_surgery] >> merge_df  >> calculate_drugs_price >> calculate_days_different >> [calculate_doctor_visit,
                                                                                                           calculate_room_price,
                                                                                                           calculate_surgery_price,
                                                                                                           calculate_lab_price,
                                                                                                           calculate_infus_price] >> calculate_total_price >> calculate_revenue >> add_is_DBD >> create_time_created >> execution >> saving_df