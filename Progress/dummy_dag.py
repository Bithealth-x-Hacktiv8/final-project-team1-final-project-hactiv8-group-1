import pandas as pd
import psycopg2
import re
from datetime import datetime

connection = psycopg2.connect(
    dbname = 'final_project',
    user = 'postgres',
    password = '123',
    host = 'localhost',
    port = '5433'
)

cursor = connection.cursor()

#cursor.execute('select * from drugs')
#rows = cursor.fetchall()
#for row in rows:
#    print(row)

def hospital():
    cursor = connection.cursor()
    cursor.execute('select * from hospital')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()

    df = pd.DataFrame(result, columns = ["id", "date_in", "date_out", "branch", "hospital_care", "drug_quantity", "admin_price", "cogs", "payment"
                                         , "review", "patient_id", "room_id", "drug_id", "doctor_id", "surgery_id", "lab_id"])
    
    return df

def patient():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from patient
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()

    df = pd.DataFrame(result, columns=['patient_id', 'patient_name', 'gender', 'age'])

    return df

def doctor():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from doctor
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()

    df = pd.DataFrame(result, columns = ['doctor_id', 'doctor', 'doctor_price'])

    return df

def drugs():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from drugs
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()

    df = pd.DataFrame(result, columns = ['drug_id', 'drug_brand', 'drug_type', 'drug_price'])

    return df

def lab():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from lab
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()
    df = pd.DataFrame(result, columns = ['lab_id', 'lab', 'lab_price'])

    return df

def room():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from room
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()
    df = pd.DataFrame(result, columns = ['room_id', 'room_type', 'food_price', 'room_price'])

    return df

def surgery():
    cursor = connection.cursor()
    cursor.execute('''
                    select * from surgery
                   ''')
    result = cursor.fetchall()
    cursor.close()
    #connection.close()
    df = pd.DataFrame(result, columns = ['surgery_id', 'surgery', 'surgery_price'])

    return df

df_hospital = hospital()
df_patient = patient()
df_surgery = surgery()
df_doctor = doctor()
df_drugs = drugs()
df_room = room()
df_lab = lab()

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

df_hospital = df_hospital.merge(df_patient, how = 'left', on = ['patient_id'])
df_hospital = df_hospital.merge(df_room, how = 'left', on = 'room_id')
df_hospital = df_hospital.merge(df_drugs, how = 'left', on = 'drug_id')
df_hospital = df_hospital.merge(df_doctor, how = 'left', on = 'doctor_id')
df_hospital = df_hospital.merge(df_surgery, how = 'left', on = 'surgery_id')
df_hospital = df_hospital.merge(df_lab, how = 'left', on = 'lab_id')



def drugs_price(df):
    drug_price_list = []
    df['drug_price'] = df['drug_price'].str.replace(r'(Rp|,00|\.)', '', regex=True).astype(int)
    for i in range(len(df)):
        drug_price = df.loc[i,'drug_quantity'] * df.loc[i,'drug_price']
        drug_price_list.append(drug_price)
    return drug_price_list


#print(df_hospital[['drug_brand', 'drug_quantity', 'drug_price', 'drug_total_price']])


def days_diff(df):
    days_list = []
    for i in range(len(df)):
        if (df.loc[i,'hospital_care'] == 'Rawat Inap'):
            days = (((pd.to_datetime(df.loc[i,'date_out']) - pd.to_datetime(df.loc[i,'date_in'])).total_seconds())/(3600*24))+1
            days_list.append(days)
        elif (df.loc[i,'hospital_care'] == 'Rawat Jalan'):
            days = '-'
            days_list.append(days)
    return days_list

def doctor_visit(df):
    doctor_visit_list = []
    df['doctor_price'] = df['doctor_price'].str.replace(r'(Rp|,00|\.)', '', regex = True).astype(int)
    for i in range(len(df)):
        if (df.loc[i, 'hospital_care'] == 'Rawat Inap'):
            if df.loc[i, 'doctor'] == 'Bedah':
               doctor_price = df.loc[i, 'days_diff'] * df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df.loc[i, 'doctor'] == 'Gigi':
               doctor_price = df.loc[i, 'days_diff'] * df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df.loc[i, 'doctor'] == 'Penyakit Dalam':
               doctor_price = df.loc[i, 'days_diff'] * df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df.loc[i, 'doctor'] == 'Kandungan':
               doctor_price = df.loc[i, 'days_diff'] * df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df.loc[i, 'doctor'] == 'Umum':
               doctor_price = df.loc[i, 'days_diff'] * df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
        elif (df.loc[i, 'hospital_care'] == 'Rawat Jalan'):
            if df.loc[i, 'doctor'] == 'Bedah':
               doctor_price = df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df.loc[i, 'doctor'] == 'Gigi':
               doctor_price = df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df.loc[i, 'doctor'] == 'Penyakit Dalam':
               doctor_price = df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df.loc[i, 'doctor'] == 'Kandungan':
               doctor_price = df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
            elif df.loc[i, 'doctor'] == 'Umum':
               doctor_price = df.loc[i, 'doctor_price']
               doctor_visit_list.append(doctor_price)
    return doctor_visit_list

def room_price_total(df):
    room_price_list = []
    food_price_list = []
    for j in range(len(df)):
        if df['room_type'].notnull()[j]:
           df.loc[j, 'room_price'] = df.loc[j, 'room_price'].replace('Rp', '').replace(',00','').replace('.','')
           df.loc[j, 'food_price'] = df.loc[j, 'food_price'].replace('Rp', '').replace(',00','').replace('.','')
           

    for i in range(len(df)):
        if df.loc[i, 'room_type'] == 'VIP':
           room_price = int(df.loc[i, 'days_diff']) * int(df.loc[i,'room_price'])
           room_price_list.append(room_price)
        elif df.loc[i, 'room_type'] == 'Kelas 1':
           room_price = int(df.loc[i, 'days_diff']) * int(df.loc[i,'room_price'])
           room_price_list.append(room_price)
        elif df.loc[i, 'room_type'] == 'Kelas 2':
           room_price = int(df.loc[i, 'days_diff']) * int(df.loc[i,'room_price'])
           room_price_list.append(room_price)
        elif df.loc[i, 'room_type'] == 'Kelas 3':
           room_price = int(df.loc[i, 'days_diff']) * int(df.loc[i,'room_price'])
           room_price_list.append(room_price)
        else:
           room_price = 0
           room_price_list.append(room_price)
    for i in range(len(df)):
        if df.loc[i, 'room_type'] == 'VIP':
           food_price = (df.loc[i, 'days_diff']) * int(df.loc[i,'food_price'])
           food_price_list.append(food_price)
        elif df.loc[i, 'room_type'] == 'Kelas 1':
           food_price = (df.loc[i, 'days_diff']) * int(df.loc[i,'food_price'])
           food_price_list.append(food_price)
        elif df.loc[i, 'room_type'] == 'Kelas 2':
           food_price = (df.loc[i, 'days_diff']) * int(df.loc[i,'food_price'])
           food_price_list.append(food_price)
        elif df.loc[i, 'room_type'] == 'Kelas 3':
           food_price = (df.loc[i, 'days_diff']) * int(df.loc[i,'food_price'])
           food_price_list.append(food_price)
        else:
           food_price = 0
           food_price_list.append(food_price)
    
    return room_price_list, food_price_list

def surgery_price_total(df):
    surgery_price_list = []
    for j in range(len(df)):
        if df['surgery'].notnull()[j]:
           df.loc[j, 'surgery_price'] = df.loc[j, 'surgery_price'].replace('Rp', '').replace(',00','').replace('.','')
    
    for i in range(len(df)):
        if df.loc[i,'surgery'] == 'Kecil':
           surgery_price = int(df.loc[i, 'surgery_price'])
           surgery_price_list.append(surgery_price)
        elif df.loc[i,'surgery'] == 'Besar':
           surgery_price = int(df.loc[i, 'surgery_price'])
           surgery_price_list.append(surgery_price)
        elif df.loc[i,'surgery'] == 'Kusus':
           surgery_price = int(df.loc[i, 'surgery_price'])
           surgery_price_list.append(surgery_price)
        else:
           surgery_price = 0
           surgery_price_list.append(surgery_price)
    
    return surgery_price_list



def lab_price_total(df):
    lab_price_list = []
    for j in range(len(df)):
        if df['lab'].notnull()[j]:
           df.loc[j, 'lab_price'] = df.loc[j, 'lab_price'].replace('Rp', '').replace(',00','').replace('.','')
    
    for i in range(len(df)):
        if df.loc[i,'lab'] == 'Hematologi':
           lab_price = int(df.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        elif df.loc[i,'lab'] == 'Kimia Darah':
           lab_price = int(df.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        elif df.loc[i,'lab'] == 'Rontgen':
           lab_price = int(df.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        elif df.loc[i,'lab'] == 'Serologi':
           lab_price = int(df.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        elif df.loc[i,'lab'] == 'Urinalisa':
           lab_price = int(df.loc[i, 'lab_price'])
           lab_price_list.append(lab_price)
        else:
           lab_price = 0
           lab_price_list.append(lab_price)
    
    return lab_price_list    

def infus_price_total(df):
    infus_price_list = []
    for i in range(len(df)):
        if df.loc[i, 'hospital_care'] == 'Rawat Inap':
           infus_price = df.loc[i, 'days_diff'] * 165000
           infus_price_list.append(infus_price)
        
        else:
           infus_price = 0
           infus_price_list.append(infus_price)

    return infus_price_list

def total_price_all(df):
    df['admin_price'] = df['admin_price'].str.replace(r'(Rp|,00|\.)','', regex = True).astype(int)
    total_amount_list = df['drug_price_total'].astype(int) + df_hospital['doctor_visit_price'].astype(int) + df_hospital['room_price_total'].astype(int) + df_hospital['food_price_total'].astype(int) + df_hospital['surgery_price_total'].astype(int) + df_hospital['lab_price_total'].astype(int) + df_hospital['infus_price_total'].astype(int) + df_hospital['admin_price'].astype(int)
    
    return total_amount_list

def total_revenue(df):
    df['cogs'] = df['cogs'].str.replace(r'(Rp|,00|\.)','', regex = True).astype(int)
    revenue_list = df['total_amount'].astype(int) - df['cogs'].astype(int)
    return revenue_list

def time_created(df):
    time_list = []
    while len(time_list) < len(df):
          time_list.append(datetime.now())
    return time_list

def save_local_csv(df):
    return df.to_csv('C:\My_File\gradexpert2024\Final Project\csv\hasil_dummy.csv', index = False)
        

df_hospital['drug_price_total'] = drugs_price(df_hospital)
df_hospital['days_diff'] = days_diff(df_hospital)
df_hospital['doctor_visit_price'] = doctor_visit(df_hospital)
df_hospital['room_price_total'], df_hospital['food_price_total'] = room_price_total(df_hospital)
df_hospital['surgery_price_total'] = surgery_price_total(df_hospital)
df_hospital['lab_price_total'] =  lab_price_total(df_hospital)
df_hospital['infus_price_total'] = infus_price_total(df_hospital)
df_hospital['total_amount'] = total_price_all(df_hospital)
df_hospital['revenue'] = total_revenue(df_hospital)
df_hospital['created_at'] = time_created(df_hospital)

save_local_csv(df_hospital)

print(df_hospital)





#print(df_hospital[['room_type','room_price','food_price','room_price_total', 'food_price_total']])
#print(df_hospital['room_type'])


#print(df_hospital['doctor_price'])
#df_hospital['days_diff'] = (((pd.to_datetime(df_hospital['date_out']) - pd.to_datetime(df_hospital['date_in'])).dt.total_seconds())/(3600*24))+1
#print(df_hospital[['date_in', 'date_out','days_diff']])