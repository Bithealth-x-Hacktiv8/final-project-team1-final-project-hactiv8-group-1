--Surgery

create table surgery (
			 surgery_id int primary key,
			 surgery varchar,
			 surgery_price money
)

COPY surgery(surgery, surgery_price, surgery_id)
FROM 'C:\Users\Arya Sangga Buana\Downloads\surgery.csv'
DELIMITER ',' 
CSV HEADER


------------------------------------------------------------------------------

--Patient

create table patient(
		patient_id int primary key,
		patient_name varchar(255),
		gender varchar(255),
		age int
)

COPY patient(patient_name, age, gender, id)
FROM 'C:\Users\Arya Sangga Buana\Downloads\patient (1).csv'
DELIMITER ',' 
CSV HEADER

---------------------------------------------------------------------------


--Doctor

create table doctor (
			doctor_id int primary key,
			doctor varchar,
			doctor_price money
)

COPY doctor(doctor, doctor_price, doctor_id)
FROM 'C:\Users\Arya Sangga Buana\Downloads\doctor.csv'
DELIMITER ',' 
CSV HEADER

---------------------------------------------------------------------------

--Drugs

create table drugs(
			drug_id int primary key,
			drug_brand varchar,
			drug_type varchar,
			drug_price money
)

COPY drugs(drug_brand, drug_type, drug_price, drug_id)
FROM 'C:\Users\Arya Sangga Buana\Downloads\drugs.csv'
DELIMITER ',' 
CSV HEADER

---------------------------------------------------------------------------

--Lab

create table lab (
			lab_id int primary key,
			lab varchar,
			lab_price money
)

COPY lab(lab, lab_price, lab_id)
FROM 'C:\Users\Arya Sangga Buana\Downloads\lab.csv'
DELIMITER ',' 
CSV HEADER

---------------------------------------------------------------------------

--Room

create table room (
			room_id int primary key,
			room varchar,
			food_price money,
			room_price money
)

COPY room(room, food_price, room_price, room_id)
FROM 'C:\Users\Arya Sangga Buana\Downloads\room_new.csv'
DELIMITER ',' 
CSV HEADER

---------------------------------------------------------------------------

--Hospital

create table hospital (
	id	int primary key,
	date_ind date,
	date_out date,
	branch varchar,
	hospital_care varchar,
	drug_quantity int,
	admin money,
	cogs money,
	payment varchar,
	review varchar,
	patient_id int,
	room_id int,
	drug_id int,
	doctor_id int,
	surgery_id int,
	lab_id int
)

COPY hospital(id, date_ind, date_out, branch, hospital_care, drug_quantity, admin, cogs, payment, review, patient_id, room_id, drug_id, doctor_id, surgery_id, lab_id)
FROM 'C:\Users\Arya Sangga Buana\Downloads\hospital_new.csv'
DELIMITER ',' 
CSV HEADER