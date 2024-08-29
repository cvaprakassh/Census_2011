#!/usr/bin/env python
# coding: utf-8

# In[14]:


pip install pandas # install pandas using pip


# In[306]:


pip install 'pymongo[srv]' #install pymongo for using MongoDB


# In[317]:


pip install sqlalchemy #install Sql alchemy


# In[2]:


pip install streamlit_jupyter #install streamlit to display the Output


# In[28]:


#import nescessary modules for the Project
import pandas as pd
import os as os
import numpy as np
import streamlit as st
from pymongo import MongoClient
from sqlalchemy import create_engine, Column, Integer, String,text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# In[4]:


#Read the data from the excel as the Dataframe
df=pd.read_excel('/Users/sivaprakash/Desktop/Guvi/Project/census_2011.xlsx')


# In[8]:


df['State/UT'] # Checking the dataframe by displaying the State/UT


# In[7]:


#Task 1: Rename the Column names

df.rename(columns={'State name':'State/UT','District name':'District','Male_Literate':'Literate_Male',
                   'Female_Literate':'Literate_Female','Rural_Households':'Households_Rural','Urban_Households':'Households_Urban','Age_Group_0_29':'Young_and_Adult','Age_Group_30_49':'Middle_Aged','Age_Group_50':'Senior_Citizen','Age not stated':'Age_Not_Stated'}, inplace=True)
df.rename(columns={'Literate_female':'Literate_Female'}, inplace=True)


# In[9]:


#Task 2: Rename State/UT Names

df['State/UT']=df['State/UT'].str.upper()
ste=df['State/UT'].copy()
state=ste.str.split(' ')
for x in state:
    for i in range(len(x)):
        if x[i].upper()=='AND':
            x[i]=x[i].lower()
            #print(x)
        else:
            x[i]=x[i].upper().title()
            
ste=state.str.join(" ")
#print(ste)
df['State/UT']=ste
#list(df['State/UT'].where(df['State/UT'].str.contains('and')))
df['State/UT']


# In[10]:


#Task 3: New State/UT formation
#Ladak formation
df['State/UT']=np.where(df['District']=="Leh(Ladakh)",'Ladakh',df['State/UT'])
df['State/UT']=np.where(df['District']=="Kargil",'Ladakh',df['State/UT'])
df.query('District=="Leh(Ladakh)" or District=="Kargil"')


# In[11]:


#Telugana District formation
T_district=['Adilabad','Nizamabad','Karimnagar','Medak','Hyderabad','Rangareddy','Mahbubnagar','Nalgonda','Warangal','Khammam']
df['State/UT']=np.where(df['District'].isin(T_district),'Telangana',df['State/UT'])
df[df['District'].isin(T_district)]


# In[20]:


#Task 4: Find and process Missing Data
#Storing Missing Values
missing_values_before=df.isnull().mean().round(4) * 100
print(missing_values_before)
df['Population'].isnull().sum()
#pd.isnull(df['Population']).sum()


# In[13]:


#Process the Missing Data
val=df['Male']+df['Female']
df['Population']=df['Population'].fillna(value=val)

l=['Literate_Male','Literate_Female']
l_value=df[l].sum(axis=1)
df['Literate']=df['Literate'].fillna(value=l_value)

h=['Households_Rural','Households_Urban']
h_value=df[h].sum(axis=1)
df['Households']=df['Households'].fillna(value=h_value)


# In[17]:


p=['Young_and_Adult','Middle_Aged','Senior_Citizen','Age_Not_Stated']
p_value=df[p].sum(axis=1)
df['Population']=df['Population'].fillna(value=p_value)
#print(check)


# In[18]:


#Storing missing values after Processing
missing_values_after=df.isnull().mean().round(4) * 100
missing_values_after


# In[19]:


#Comparison of missing data before and after processing
comparison=missing_values_before-missing_values_after
comparison


# In[2]:


#Task 5: Save Data to MongoDB
Client=MongoClient('localhost',27017) #opening Mongo DB using Mongo Client
db=Client.p1_database #creating the Database
collection=db.census #creating the Collection name census


# In[22]:


df.reset_index(inplace=True)
data_dict=df.to_dict("records")#converting all the record to Dictionary to insert in to Mongo DB
data_dict


# In[28]:


collection.insert_many(data_dict) #Inserting the records using insert Many


# In[384]:


#Client.close()


# In[29]:


collection.find_one() #checking the MOngo DB by checking the Collecition using find_one()


# In[61]:


#Task 6: Database connection and data upload
#establish the connection with SQL using SQL alchemy
password="Geethu%40101" 
DATABASE_URL = "mysql+mysqlconnector://root:Geethu%40101@localhost/M32_Project_1"
engine = create_engine(DATABASE_URL) #create the engine using the database URL to establish the connection with SQL
Session = sessionmaker(bind=engine)
sql_session = Session()
Base = declarative_base()


# In[80]:


#creating the Columns for the Table
class Table(Base):
    __tablename__ = 'census_2011'
    
    id = Column(Integer, primary_key=True)
    District_code = Column(Integer)
    State_UT = Column(String(100))
    District = Column(String(100))
    Population= Column(Integer)
    Male = Column(Integer)
    Female=Column(Integer)
    Literate=Column(Integer)
    Literate_Male=Column(Integer)
    Literate_Female=Column(Integer)
    SC=Column(Integer)
    Male_SC=Column(Integer)
    Female_SC=Column(Integer)
    ST=Column(Integer)
    Male_ST=Column(Integer)
    Female_ST=Column(Integer)
    Workers=Column(Integer)
    Male_Workers=Column(Integer)
    Female_Workers=Column(Integer)
    Main_Workers=Column(Integer)
    Marginal_Workers=Column(Integer)
    Non_Workers=Column(Integer)
    Cultivator_Workers=Column(Integer)
    Agricultural_Workers=Column(Integer)
    Household_Workers=Column(Integer)
    Other_Workers=Column(Integer)
    Hindus=Column(Integer)
    Muslims=Column(Integer)
    Christians=Column(Integer)
    Sikhs=Column(Integer)
    Buddhists=Column(Integer)
    Jains=Column(Integer)
    Others_Religions=Column(Integer)
    Religion_Not_Stated=Column(Integer)
    LPG_or_PNG_Households=Column(Integer)
    Housholds_with_Electric_Lighting=Column(Integer)
    Households_with_Internet=Column(Integer)
    Households_with_Computer=Column(Integer)
    Households_Rural=Column(Integer)
    Households_Urban=Column(Integer)
    Households=Column(Integer)
    Below_Primary_Education=Column(Integer)
    Primary_Education=Column(Integer)
    Middle_Education=Column(Integer)
    Secondary_Education=Column(Integer)
    Higher_Education=Column(Integer)
    Graduate_Education=Column(Integer)
    Other_Education=Column(Integer)
    Literate_Education=Column(Integer)
    Illiterate_Education=Column(Integer)
    Total_Education=Column(Integer)
    Young_and_Adult=Column(Integer)
    Middle_Aged=Column(Integer)
    Senior_Citizen=Column(Integer)
    Age_Not_Stated=Column(Integer)
    Households_with_Bicycle=Column(Integer)
    Households_with_Car_Jeep_Van=Column(Integer)
    Households_with_Radio_Transistor=Column(Integer)
    Households_with_Scooter_Motorcycle_Moped=Column(Integer)
    Households_with_Telephone_Mobile_Phone_Landline_only=Column(Integer)
    Households_with_Telephone_Mobile_Phone_Mobile_only=Column(Integer)
    H_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car=Column(Integer)
    Households_with_Television=Column(Integer)
    Households_with_Telephone_Mobile_Phone=Column(Integer)
    Households_with_Telephone_Mobile_Phone_Both=Column(Integer)
    Condition_of_occupied_census_houses_Dilapidated_Households=Column(Integer)
    Households_with_separate_kitchen_Cooking_inside_house=Column(Integer)
    Having_bathing_facility_Total_Households=Column(Integer)
    Having_latrine_facility_within_the_premises_Total_Households=Column(Integer)
    Ownership_Owned_Households=Column(Integer)
    Ownership_Rented_Households=Column(Integer)
    Type_of_bathing_facility_Enclosure_without_roof_Households=Column(Integer)
    Type_of_fuel_used_for_cooking_Any_other_Households=Column(Integer)
    Type_of_latrine_facility_Pit_latrine_Households=Column(Integer)
    Type_of_latrine_facility_Other_latrine_Households=Column(Integer)
    Night_soil_disposed_into_open_drain_Households=Column(Integer)
    Flush_pour_flush_latrine_connected_to_other_system_Households=Column(Integer)
    Not_having_bathing_facility_within_the_premises_Total_Households=Column(Integer)
    Not_having_latrine_facility_Alternative_source_Open_Households=Column(Integer)
    Main_source_of_drinking_water_Un_covered_well_Households=Column(Integer)
    Main_source_Dw_Handpump_Tubewell_Borewell_Households=Column(Integer)
    Main_source_of_drinking_water_Spring_Households=Column(Integer)
    Main_source_of_drinking_water_River_Canal_Households=Column(Integer)
    Main_source_of_drinking_water_Other_sources_Households=Column(Integer)
    DW_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households=Column(Integer)
    L_drinking_water_source_Near_the_premises_Households=Column(Integer)
    L_drinking_water_source_Within_the_premises_Households=Column(Integer)
    Main_source_of_dW_Tank_Pond_Lake_Households=Column(Integer)
    Main_source_of_dW_Tapwater_Households=Column(Integer)
    Main_source_of_dW_Tubewell_Borehole_Households=Column(Integer)
    Household_size_1_person_Households=Column(Integer)
    Household_size_2_persons_Households=Column(Integer)
    Household_size_1_to_2_persons=Column(Integer)
    Household_size_3_persons_Households=Column(Integer)
    Household_size_3_to_5_persons_Households=Column(Integer)
    Household_size_4_persons_Households=Column(Integer)
    Household_size_5_persons_Households=Column(Integer)
    Household_size_6_8_persons_Households=Column(Integer)
    Household_size_9_persons_and_above_Households=Column(Integer)
    Location_of_drinking_water_source_Away_Households=Column(Integer)
    Married_couples_1_Households=Column(Integer)
    Married_couples_2_Households=Column(Integer)
    Married_couples_3_Households=Column(Integer)
    Married_couples_3_or_more_Households=Column(Integer)
    Married_couples_4_Households=Column(Integer)
    Married_couples_5__Households=Column(Integer)
    Married_couples_None_Households=Column(Integer)
    Power_Parity_Less_than_Rs_45000=Column(Integer)
    Power_Parity_Rs_45000_90000=Column(Integer)
    Power_Parity_Rs_90000_150000=Column(Integer)
    Power_Parity_Rs_45000_150000=Column(Integer)
    Power_Parity_Rs_150000_240000=Column(Integer)
    Power_Parity_Rs_240000_330000=Column(Integer)
    Power_Parity_Rs_150000_330000=Column(Integer)
    Power_Parity_Rs_330000_425000=Column(Integer)
    Power_Parity_Rs_425000_545000=Column(Integer)
    Power_Parity_Rs_330000_545000=Column(Integer)
    Power_Parity_Above_Rs_545000=Column(Integer)
    Total_Power_Parity=Column(Integer)
    # Add more columns as needed

# Create the table
Base.metadata.create_all(engine)


# In[81]:


#getting the Mongo Data fields
mongo_data=list(collection.find())
mongo_key=collection.find_one()
key=[]
for k in mongo_key:
    key.append(k)
print(mongo_key)


# In[82]:


#Mapping the table column and Fields of Mongo Database 
def transform_document(doc):
    return Table(
        id=doc.get('id'),
        District_code=doc.get('District code'),
        State_UT= doc.get('State/UT'),
        District=doc.get('District' ),
        Population=doc.get('Population'),
        Male=doc.get('Male'),
        Female= doc.get('Female'),
        Literate=doc.get('Literate'),
        Literate_Male=doc.get('Literate_Male'),
        Literate_Female=doc.get('Literate_Female'),
        SC=doc.get('SC'),
        Male_SC=doc.get('Male_SC'),
        Female_SC=doc.get('Female_SC'),
        ST=doc.get('ST'),
        Male_ST=doc.get('Male_ST'),
        Female_ST=doc.get('Female_ST'),
        Workers=doc.get('Workers'),
        Male_Workers=doc.get('Male_Workers'),
        Female_Workers=doc.get('Female_Workers'),
        Main_Workers=doc.get('Main_Workers'),
        Marginal_Workers=doc.get('Marginal_Workers'),
        Non_Workers=doc.get('Non_Workers'),
        Cultivator_Workers=doc.get('Cultivator_Workers'),
        Agricultural_Workers= doc.get('Agricultural_Workers'),
        Household_Workers=doc.get('Household_Workers'),
        Other_Workers=doc.get('Other_Workers'),
        Hindus= doc.get('Hindus'),
        Muslims=doc.get('Muslims'),
        Christians=doc.get('Christians'),
        Sikhs=doc.get('Sikhs'),
        Buddhists=doc.get('Buddhists'),
        Jains=doc.get('Jains'),
        Others_Religions=doc.get('Others_Religions'),
        Religion_Not_Stated=doc.get('Religion_Not_Stated'),
        LPG_or_PNG_Households=doc.get('LPG_or_PNG_Households'),
        Housholds_with_Electric_Lighting=doc.get('Housholds_with_Electric_Lighting'),
        Households_with_Internet=doc.get('Households_with_Internet'),
        Households_with_Computer=doc.get('Households_with_Computer'),
        Households_Rural=doc.get('Households_Rural'),
        Households_Urban=doc.get('Households_Urban'),
        Households=doc.get('Households'),
        Below_Primary_Education=doc.get('Below_Primary_Education'),
        Primary_Education=doc.get('Primary_Education'),
        Middle_Education=doc.get('Middle_Education'),
        Secondary_Education=doc.get('Secondary_Education'),
        Higher_Education=doc.get('Higher_Education'),
        Graduate_Education=doc.get('Graduate_Education'),
        Other_Education=doc.get('Other_Education'),
        Literate_Education=doc.get('Literate_Education'),
        Illiterate_Education=doc.get('Illiterate_Education'),
        Total_Education=doc.get('Total_Education'),
        Young_and_Adult=doc.get('Young_and_Adult'),
        Middle_Aged=doc.get('Middle_Aged'),
        Senior_Citizen=doc.get('Senior_Citizen'),
        Age_Not_Stated=doc.get('Age_Not_Stated'),
        Households_with_Bicycle=doc.get('Households_with_Bicycle'),
        Households_with_Car_Jeep_Van=doc.get('Households_with_Car_Jeep_Van'),
        Households_with_Radio_Transistor=doc.get('Households_with_Radio_Transistor'),
        Households_with_Scooter_Motorcycle_Moped=doc.get('Households_with_Scooter_Motorcycle_Moped'),
        Households_with_Telephone_Mobile_Phone_Landline_only=doc.get('Households_with_Telephone_Mobile_Phone_Landline_only'),
        Households_with_Telephone_Mobile_Phone_Mobile_only=doc.get('Households_with_Telephone_Mobile_Phone_Mobile_only'),
        H_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car=doc.get('Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car'),
        Households_with_Television=doc.get('Households_with_Television'),
        Households_with_Telephone_Mobile_Phone=doc.get('Households_with_Telephone_Mobile_Phone'),
        Households_with_Telephone_Mobile_Phone_Both=doc.get('Households_with_Telephone_Mobile_Phone_Both'),
        Condition_of_occupied_census_houses_Dilapidated_Households=doc.get('Condition_of_occupied_census_houses_Dilapidated_Households'),
        Households_with_separate_kitchen_Cooking_inside_house=doc.get('Households_with_separate_kitchen_Cooking_inside_house'),
        Having_bathing_facility_Total_Households=doc.get('Having_bathing_facility_Total_Households'),
        Having_latrine_facility_within_the_premises_Total_Households=doc.get('Having_latrine_facility_within_the_premises_Total_Households'),
        Ownership_Owned_Households=doc.get('Ownership_Owned_Households'),
        Ownership_Rented_Households=doc.get('Ownership_Rented_Households'),
        Type_of_bathing_facility_Enclosure_without_roof_Households=doc.get('Type_of_bathing_facility_Enclosure_without_roof_Households'),
        Type_of_fuel_used_for_cooking_Any_other_Households=doc.get('Type_of_fuel_used_for_cooking_Any_other_Households'),
        Type_of_latrine_facility_Pit_latrine_Households=doc.get('Type_of_latrine_facility_Pit_latrine_Households'),
        Type_of_latrine_facility_Other_latrine_Households=doc.get('Type_of_latrine_facility_Other_latrine_Households'),
        Night_soil_disposed_into_open_drain_Households=doc.get('Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households'),
        Flush_pour_flush_latrine_connected_to_other_system_Households=doc.get('Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households'),
        Not_having_bathing_facility_within_the_premises_Total_Households=doc.get('Not_having_bathing_facility_within_the_premises_Total_Households'),
        Not_having_latrine_facility_Alternative_source_Open_Households=doc.get('Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households'),
        Main_source_of_drinking_water_Un_covered_well_Households=doc.get('Main_source_of_drinking_water_Un_covered_well_Households'),
        Main_source_Dw_Handpump_Tubewell_Borewell_Households=doc.get('Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households'),
        Main_source_of_drinking_water_Spring_Households=doc.get('Main_source_of_drinking_water_Spring_Households'),
        Main_source_of_drinking_water_River_Canal_Households=doc.get('Main_source_of_drinking_water_River_Canal_Households'),
        Main_source_of_drinking_water_Other_sources_Households=doc.get('Main_source_of_drinking_water_Other_sources_Households'),
        DW_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households=doc.get('Main_source_of_drinking_water_Other_sources_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households'),
        L_drinking_water_source_Near_the_premises_Households=doc.get('Location_of_drinking_water_source_Near_the_premises_Households'),
        L_drinking_water_source_Within_the_premises_Households=doc.get('Location_of_drinking_water_source_Within_the_premises_Households'),
        Main_source_of_dW_Tank_Pond_Lake_Households=doc.get('Main_source_of_drinking_water_Tank_Pond_Lake_Households'),
        Main_source_of_dW_Tapwater_Households=doc.get('Main_source_of_drinking_water_Tapwater_Households'),
        Main_source_of_dW_Tubewell_Borehole_Households=doc.get('Main_source_of_drinking_water_Tubewell_Borehole_Households'),
        Household_size_1_person_Households=doc.get('Household_size_1_person_Households'),
        Household_size_2_persons_Households=doc.get('Household_size_2_persons_Households'),
        Household_size_1_to_2_persons=doc.get('Household_size_1_to_2_persons'),
        Household_size_3_persons_Households=doc.get('Household_size_3_persons_Households'),
        Household_size_3_to_5_persons_Households=doc.get('Household_size_3_to_5_persons_Households'),
        Household_size_4_persons_Households=doc.get('Household_size_4_persons_Households'),
        Household_size_5_persons_Households=doc.get('Household_size_5_persons_Households'),
        Household_size_6_8_persons_Households=doc.get('Household_size_6_8_persons_Households'),
        Household_size_9_persons_and_above_Households=doc.get('Household_size_9_persons_and_above_Households'),
        Location_of_drinking_water_source_Away_Households=doc.get('Location_of_drinking_water_source_Away_Households'),
        Married_couples_1_Households=doc.get('Married_couples_1_Households'),
        Married_couples_2_Households=doc.get('Married_couples_2_Households'),
        Married_couples_3_Households=doc.get('Married_couples_3_Households'),
        Married_couples_3_or_more_Households=doc.get('Married_couples_3_or_more_Households'),
        Married_couples_4_Households=doc.get('Married_couples_4_Households'),
        Married_couples_5__Households=doc.get('Married_couples_5__Households'),
        Married_couples_None_Households=doc.get('Married_couples_None_Households'),
        Power_Parity_Less_than_Rs_45000=doc.get('Power_Parity_Less_than_Rs_45000'),
        Power_Parity_Rs_45000_90000=doc.get('Power_Parity_Rs_45000_90000'),
        Power_Parity_Rs_90000_150000=doc.get('Power_Parity_Rs_90000_150000'),
        Power_Parity_Rs_45000_150000=doc.get('Power_Parity_Rs_45000_150000'),
        Power_Parity_Rs_150000_240000=doc.get('Power_Parity_Rs_150000_240000'),
        Power_Parity_Rs_240000_330000=doc.get('Power_Parity_Rs_240000_330000'),
        Power_Parity_Rs_150000_330000=doc.get('Power_Parity_Rs_150000_330000'),
        Power_Parity_Rs_330000_425000=doc.get('Power_Parity_Rs_330000_425000'),
        Power_Parity_Rs_425000_545000=doc.get('Power_Parity_Rs_425000_545000'),
        Power_Parity_Rs_330000_545000=doc.get('Power_Parity_Rs_330000_545000'),
        Power_Parity_Above_Rs_545000=doc.get('Power_Parity_Above_Rs_545000'),
        Total_Power_Parity=doc.get('Total_Power_Parity'),
            
        # Add more fields as needed
    )


# In[83]:


# Clean data by replacing NaN with None
mongo_data_collection = [
    {k: (None if pd.isna(v) else v) for k, v in record.items()}
    for record in mongo_data
]
mongo_data_collection


# In[84]:


# Transform data
mysql_data = [transform_document(doc) for doc in mongo_data_collection]
#mysql_data


# In[77]:


sql_session.rollback() #if error occurs we can rollback any time


# In[85]:


# Insert data into MySQL
sql_session.bulk_save_objects(mysql_data)
sql_session.commit()


# In[86]:


result = sql_session.execute(text(('SELECT * FROM census_2011')))

for row in result:
    print(row)


# In[21]:


st.title("Census 2011 Data")


# In[34]:


#Q1 Population
t1=sql_session.execute(text(('Select District,Population from census_2011')))
for row in t1:
    print(row)


# In[20]:


#Task 7 : Q1)Total Population of Each District

query ='Select District,Population from census_2011'
t1 = pd.read_sql(query, engine)


# In[36]:


#Q2)literate males and Literate Females in Each District

q2='Select District,Literate_Male,Literate_Female from census_2011'
t2=pd.read_sql(q2,engine)
t2


# In[38]:


#Q3 percentage of workers(both male and female)in each district
q3='select District,Male_Workers,((Male_workers/Workers)*100) as percentage_Male_workers,Female_Workers,((Female_workers/Workers)*100) as percantage_Female_workers from census_2011;'
t3=pd.read_sql(q3,engine)
t3


# In[39]:


#Q4 households have access to LPG or PNG as a cooking fuel in each district
q4='select District, LPG_or_PNG_Households from census_2011;'
t4=pd.read_sql(q4,engine)
t4


# In[42]:


#Q5 Religious composition (Hindus,Muslims,Christians,etc.) of each district
q5='select District,(Hindus/population)*100 as Hindus,(Muslims/population)*100 as Muslims,(Christians/population)*100 as Christians,(Sikhs/population)*100 as Sikhs,(Buddhists/population)*100 as Buddhists,(Jains/population)*100 as Jains,(Others_Religions/population)*100 as Others_religions,(Religion_Not_Stated/population)*100 as Not_stated from census_2011;'
t5=pd.read_sql(q5,engine)
t5


# In[43]:


#Q6 households have internet access in each district
q6='select District,Households_with_internet from census_2011;'
t6=pd.read_sql(q6,engine)
t6


# In[44]:


#Q7 Educational attainment distribution(below primary,primary,middle, secondary, etc.) in each district
q7='select District,below_Primary_education,Primary_Education,Middle_Education,Secondary_Education,Higher_Education,Graduate_Education,Other_Education from census_2011;'
t7=pd.read_sql(q7,engine)
t7


# In[45]:


#Q8 households have access to various modes of transportation(bicycle, car, radio, television, etc.) in each district
q8='select District,Households_with_Bicycle,Households_with_Car_Jeep_Van,Households_with_Radio_Transistor,Households_with_Television from census_2011 ;'
t8=pd.read_sql(q8,engine)
t8


# In[46]:


#Q9 condition of occupied census houses
#(dilapidated,with separate kitchen, with bathing facility, with latrine facility, etc.) in each district
q9='select District,Condition_of_occupied_census_houses_Dilapidated_Households as Dilapidated_Households,Households_with_separate_kitchen_Cooking_inside_house as seperate_Kitchen,Having_bathing_facility_Total_Households as Bathing_facility,Having_latrine_facility_within_the_premises_Total_Households as Latrine_facility from census_2011;'
t9=pd.read_sql(q9,engine)
t9


# In[47]:


#Q10 household size distributed (1 person, 2 persons, 3-5 persons, etc.)in each district
q10='select District,Household_size_1_person_Households,Household_size_2_persons_Households,Household_size_3_to_5_persons_Households from census_2011;'
t10=pd.read_sql(q10,engine)
t10


# In[49]:


#Q11 total number of households in each state
q11='select State_UT as "State/UT",sum(Households) as Households from census_2011 group by State_UT;'
t11=pd.read_sql(q11,engine)
t11


# In[50]:


#Q12 households have a latrine facility within the premises in each state
q12='select State_UT as "State/UT",sum(Having_latrine_facility_within_the_premises_Total_Households) as "latrine facility within the premises" from census_2011 group by State_UT;'
t12=pd.read_sql(q12,engine)
t12


# In[51]:


#Q13 average household size in each state
q13='select State_UT as "State/UT",avg(Household_size_1_person_Households) as Household_size_1 ,avg (Household_size_2_persons_Households)as Household_size_2 ,avg(Household_size_1_to_2_persons) as  Household_size_1_to_2,avg(Household_size_3_persons_Households) as Household_size_3,avg(Household_size_3_to_5_persons_Households)as Household_size_3_to_5,avg(Household_size_4_persons_Households)as Household_size_4,avg(Household_size_5_persons_Households)as Household_size_5,avg(Household_size_6_8_persons_Households)as Household_size_6_to_8,avg(Household_size_9_persons_and_above_Households)as Household_size_9_and_above from census_2011 group by State_UT;'
t13=pd.read_sql(q13,engine)
t13


# In[53]:


#Q14 households are owned versus rented in each state
q14='select State_UT as "State/UT",sum(Ownership_Owned_Households) as Owned , sum(Ownership_Rented_Households) as rented from census_2011 group by State_UT;'
t14=pd.read_sql(q14,engine)
t14


# In[55]:


#Q15 distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) in each state
q15='select State_UT as "State/UT",sum(Type_of_latrine_facility_Pit_latrine_Households) as Pit_Laterine_households,sum(Type_of_latrine_facility_Other_latrine_Households) as other_laterine,sum(Night_soil_disposed_into_open_drain_Households) as Night_soil_diposed,sum(Flush_pour_flush_latrine_connected_to_other_system_Households) as flush_laterine from census_2011 group by State_UT;'
t15=pd.read_sql(q15,engine)
t15


# In[57]:


#Q16 households have access to drinking water sources near the premises in each state
q16='select State_UT as "State/UT",sum(L_drinking_water_source_Near_the_premises_Households) as "households have access to drinking water sources near the premises" from census_2011 group by State_UT;'
t16=pd.read_sql(q16,engine)
t16


# In[58]:


#Q17 average household income distribution in each state based on the power parity categories
q17='select State_UT as "State/UT",avg(Power_Parity_Less_than_Rs_45000) as "Power_Parity_Less_than_Rs_45000",avg(Power_Parity_Rs_45000_90000) as "Power_Parity_Rs_45000_90000",avg(Power_Parity_Rs_90000_150000) as "Power_Parity_Rs_90000_150000" , avg(Power_Parity_Rs_45000_150000) as "Power_Parity_Rs_45000_150000",avg(Power_Parity_Rs_150000_240000) as "Power_Parity_Rs_150000_240000",avg(Power_Parity_Rs_240000_330000) as "Power_Parity_Rs_240000_330000",avg(Power_Parity_Rs_150000_330000)as "Power_Parity_Rs_150000_330000" , avg(Power_Parity_Rs_330000_425000) as "Power_Parity_Rs_330000_425000",avg(Power_Parity_Rs_425000_545000) as "Power_Parity_Rs_425000_545000",avg(Power_Parity_Above_Rs_545000) as "Power_Parity_Above_Rs_545000" from census_2011 group by State_UT;'
t17=pd.read_sql(q17,engine)
t17


# In[64]:


#Q18 he percentage of married couples with different household sizes in each state
q18='select a.State_UT as "state/UT",sum(round(100*Married_couples_1_Households/c.Total,2)) as Married_couples_1_Households,sum(round(100*Married_couples_2_Households/c.Total,2)) as Married_couples_2_Households,sum(round(100*Married_couples_3_Households/c.Total,2)) as Married_couples_3_Households,sum(round(100*Married_couples_3_or_more_Households/c.Total,2)) as Married_couples_3_or_more_Households,sum(round(100*Married_couples_4_Households/c.Total,2)) as Married_couples_4_Households,sum(round(100*Married_couples_5__Households/c.Total,2)) as Married_couples_5__Households,sum(round(100*Married_couples_None_Households/c.Total,2)) as Married_couples_None_Households from census_2011 a, (select State_UT,sum(ifnull(Married_couples_1_Households,0)+ifnull(Married_couples_2_Households,0)+ifnull(Married_couples_3_or_more_Households,0)+ifnull(Married_couples_None_Households,0)) as Total from census_2011 group by State_UT) c where c.State_UT=a.State_UT group by a.State_UT;'
t18=pd.read_sql(q18,engine)
t18


# In[65]:


#Q19 households fall below the poverty line in each state based on the power parity categories
q19='select State_UT as "state/UT",sum(Power_Parity_Less_than_Rs_45000) as Below_poverty_line_households from census_2011 group by State_UT;'
t19=pd.read_sql(q19,engine)
t19


# In[66]:


#Q20 the overall literacy rate (percentage of literate population) in each state
q20='select State_UT as "state/UT",sum(Literate) as Literate_population,sum(round(100*Literate/s.total,2)) as Percent_Literate_population from census_2011 c,(select sum(literate) as Total from census_2011) s group by State_UT;'
t20=pd.read_sql(q20,engine)
t20


# In[ ]:




