#!/usr/bin/env python
# coding: utf-8

# In[4]:


import streamlit as st 
import pandas as pd 
from sqlalchemy import create_engine


password="Geethu%40101"

DATABASE_URL = "mysql+mysqlconnector://root:Geethu%40101@localhost/M32_Project_1"
engine = create_engine(DATABASE_URL)


query ='Select District,Population from census_2011'
t1 = pd.read_sql(query, engine)

st.title("Census 2011 Data")
#1 1)Total Population of Each District
st.title("District and its Population")
st.dataframe(t1)

# Selecting numeric columns only
numeric_columns = t1.select_dtypes(include=['float64', 'int64']).columns

# Ensure there are no NaN values (which could cause mixed-type errors)
t1_numeric = t1[numeric_columns].dropna()


# In[5]:


#2)literate males and Literate Females in Each District

q2='Select District,Literate_Male,Literate_Female from census_2011'
t2=pd.read_sql(q2,engine)
st.title("literate males and Literate Females in Each District")
st.dataframe(t2)


# In[6]:


#percentage of workers(both male and female)in each district
q3='select District,Male_Workers,((Male_workers/Workers)*100) as percentage_Male_workers,Female_Workers,((Female_workers/Workers)*100) as percantage_Female_workers from census_2011;'
t3=pd.read_sql(q3,engine)
st.title("percentage of workers(both male and female)in each district")
st.dataframe(t3)


# In[7]:


#households have access to LPG or PNG as a cooking fuel in each district
q4='select District, LPG_or_PNG_Households from census_2011;'
t4=pd.read_sql(q4,engine)
st.title("households have access to LPG or PNG as a cooking fuel in each district")
st.dataframe(t4)


# In[8]:


#Religious composition (Hindus,Muslims,Christians,etc.) of each district
q5='select District,(Hindus/population)*100 as Hindus,(Muslims/population)*100 as Muslims,(Christians/population)*100 as Christians,(Sikhs/population)*100 as Sikhs,(Buddhists/population)*100 as Buddhists,(Jains/population)*100 as Jains,(Others_Religions/population)*100 as Others_religions,(Religion_Not_Stated/population)*100 as Not_stated from census_2011;'
t5=pd.read_sql(q5,engine)
st.title("Religious composition (Hindus,Muslims,Christians,etc.) of each district")
st.dataframe(t5)


# In[9]:


#households have internet access in each district
q6='select District,Households_with_internet from census_2011;'
t6=pd.read_sql(q6,engine)
st.title("households have internet access in each district")
st.dataframe(t6)


# In[10]:


#Educational attainment distribution(below primary,primary,middle, secondary, etc.) in each district
q7='select District,below_Primary_education,Primary_Education,Middle_Education,Secondary_Education,Higher_Education,Graduate_Education,Other_Education from census_2011;'
t7=pd.read_sql(q7,engine)
st.title("Educational attainment distribution(below primary,primary,middle, secondary, etc.) in each district")
st.dataframe(t7)


# In[12]:


#households have access to various modes of transportation(bicycle, car, radio, television, etc.) in each district
q8='select District,Households_with_Bicycle,Households_with_Car_Jeep_Van,Households_with_Radio_Transistor,Households_with_Television from census_2011 ;'
t8=pd.read_sql(q8,engine)
st.title("households have access to various modes of transportation(bicycle, car, radio, television, etc.) in each district")
st.dataframe(t8)


# In[13]:


#condition of occupied census houses
#houses(dilapidated,with separate kitchen, with bathing facility, with latrine facility, etc.) in each district
q9='select District,Condition_of_occupied_census_houses_Dilapidated_Households as Dilapidated_Households,Households_with_separate_kitchen_Cooking_inside_house as seperate_Kitchen,Having_bathing_facility_Total_Households as Bathing_facility,Having_latrine_facility_within_the_premises_Total_Households as Latrine_facility from census_2011;'
t9=pd.read_sql(q9,engine)
st.title("houses(dilapidated,with separate kitchen, with bathing facility, with latrine facility, etc.) in each district")
st.dataframe(t9)


# In[14]:


#household size distributed (1 person, 2 persons, 3-5 persons, etc.)in each district
q10='select District,Household_size_1_person_Households,Household_size_2_persons_Households,Household_size_3_to_5_persons_Households from census_2011;'
t10=pd.read_sql(q10,engine)
st.title("household size distributed (1 person, 2 persons, 3-5 persons, etc.)in each district")
st.dataframe(t10)


# In[15]:


#total number of households in each state
q11='select State_UT as "State/UT",sum(Households) as Households from census_2011 group by State_UT;'
t11=pd.read_sql(q11,engine)
st.title("total number of households in each state")
st.dataframe(t11)


# In[16]:


#households have a latrine facility within the premises in each state
q12='select State_UT as "State/UT",sum(Having_latrine_facility_within_the_premises_Total_Households) as "latrine facility within the premises" from census_2011 group by State_UT;'
t12=pd.read_sql(q12,engine)
st.title("households have a latrine facility within the premises in each state")
st.dataframe(t12)


# In[17]:


#average household size in each state
q13='select State_UT as "State/UT",avg(Household_size_1_person_Households) as Household_size_1 ,avg (Household_size_2_persons_Households)as Household_size_2 ,avg(Household_size_1_to_2_persons) as  Household_size_1_to_2,avg(Household_size_3_persons_Households) as Household_size_3,avg(Household_size_3_to_5_persons_Households)as Household_size_3_to_5,avg(Household_size_4_persons_Households)as Household_size_4,avg(Household_size_5_persons_Households)as Household_size_5,avg(Household_size_6_8_persons_Households)as Household_size_6_to_8,avg(Household_size_9_persons_and_above_Households)as Household_size_9_and_above from census_2011 group by State_UT;'
t13=pd.read_sql(q13,engine)
st.title("average household size in each state")
st.dataframe(t13)


# In[18]:


#households are owned versus rented in each state
q14='select State_UT as "State/UT",sum(Ownership_Owned_Households) as Owned , sum(Ownership_Rented_Households) as rented from census_2011 group by State_UT;'
t14=pd.read_sql(q14,engine)
st.title("households are owned versus rented in each state")
st.dataframe(t14)


# In[19]:


#distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) in each state
q15='select State_UT as "State/UT",sum(Type_of_latrine_facility_Pit_latrine_Households) as Pit_Laterine_households,sum(Type_of_latrine_facility_Other_latrine_Households) as other_laterine,sum(Night_soil_disposed_into_open_drain_Households) as Night_soil_diposed,sum(Flush_pour_flush_latrine_connected_to_other_system_Households) as flush_laterine from census_2011 group by State_UT;'
t15=pd.read_sql(q15,engine)
st.title("distribution of different types of latrine facilities (pit latrine, flush latrine, etc.) in each state")
st.dataframe(t15)


# In[20]:


#households have access to drinking water sources near the premises in each state
q16='select State_UT as "State/UT",sum(L_drinking_water_source_Near_the_premises_Households) as "households have access to drinking water sources near the premises" from census_2011 group by State_UT;'
t16=pd.read_sql(q16,engine)
st.title("households have access to drinking water sources near the premises in each state")
st.dataframe(t16)


# In[21]:


#average household income distribution in each state based on the power parity categories
q17='select State_UT as "State/UT",avg(Power_Parity_Less_than_Rs_45000) as "Power_Parity_Less_than_Rs_45000",avg(Power_Parity_Rs_45000_90000) as "Power_Parity_Rs_45000_90000",avg(Power_Parity_Rs_90000_150000) as "Power_Parity_Rs_90000_150000" , avg(Power_Parity_Rs_45000_150000) as "Power_Parity_Rs_45000_150000",avg(Power_Parity_Rs_150000_240000) as "Power_Parity_Rs_150000_240000",avg(Power_Parity_Rs_240000_330000) as "Power_Parity_Rs_240000_330000",avg(Power_Parity_Rs_150000_330000)as "Power_Parity_Rs_150000_330000" , avg(Power_Parity_Rs_330000_425000) as "Power_Parity_Rs_330000_425000",avg(Power_Parity_Rs_425000_545000) as "Power_Parity_Rs_425000_545000",avg(Power_Parity_Above_Rs_545000) as "Power_Parity_Above_Rs_545000" from census_2011 group by State_UT;'
t17=pd.read_sql(q17,engine)
st.title("average household income distribution in each state based on the power parity categories")
st.dataframe(t17)


# In[22]:


#the percentage of married couples with different household sizes in each state
q18='select a.State_UT as "state/UT",sum(round(100*Married_couples_1_Households/c.Total,2)) as Married_couples_1_Households,sum(round(100*Married_couples_2_Households/c.Total,2)) as Married_couples_2_Households,sum(round(100*Married_couples_3_Households/c.Total,2)) as Married_couples_3_Households,sum(round(100*Married_couples_3_or_more_Households/c.Total,2)) as Married_couples_3_or_more_Households,sum(round(100*Married_couples_4_Households/c.Total,2)) as Married_couples_4_Households,sum(round(100*Married_couples_5__Households/c.Total,2)) as Married_couples_5__Households,sum(round(100*Married_couples_None_Households/c.Total,2)) as Married_couples_None_Households from census_2011 a, (select State_UT,sum(ifnull(Married_couples_1_Households,0)+ifnull(Married_couples_2_Households,0)+ifnull(Married_couples_3_or_more_Households,0)+ifnull(Married_couples_None_Households,0)) as Total from census_2011 group by State_UT) c where c.State_UT=a.State_UT group by a.State_UT;'
t18=pd.read_sql(q18,engine)
st.title("the percentage of married couples with different household sizes in each state")
st.dataframe(t18)


# In[23]:


#households fall below the poverty line in each state based on the power parity categories
q19='select State_UT as "state/UT",sum(Power_Parity_Less_than_Rs_45000) as Below_poverty_line_households from census_2011 group by State_UT;'
t19=pd.read_sql(q19,engine)
st.title("households fall below the poverty line in each state based on the power parity categories")
st.dataframe(t19)


# In[24]:


#the overall literacy rate (percentage of literate population) in each state
q20='select State_UT as "state/UT",sum(Literate) as Literate_population,sum(round(100*Literate/s.total,2)) as Percent_Literate_population from census_2011 c,(select sum(literate) as Total from census_2011) s group by State_UT;'
t20=pd.read_sql(q20,engine)
st.title("the overall literacy rate (percentage of literate population) in each state")
st.dataframe(t20)


# In[ ]:




