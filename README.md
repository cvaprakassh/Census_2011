# Census_2011
# Project 1
# Census Data Standardization and Analysis Pipeline

# Technologies: Python, SQL , MongoDB, Streamlit

# Project Description :
The task is to clean, process, and analyze census data from a given source, including data renaming, missing data handling, state/UT name standardization, new state/UT formation handling, data storage, database connection, and querying. The goal is to ensure uniformity, accuracy, and accessibility of the census data for further analysis and visualization.

# Task 1: Rename the Column names

For uniformity in the datasets and taking into consideration the census year, we need to rename some columns.
Read the data from the Excel as dataframe using the pandas and Rename the columns .

# Task 2: Rename State/UT Names

The State/UT names are in all caps in the census data, For uniformity across datasets we use the names so that only the first character of each word in the name is in upper case and the rest are in lower case. However, if the word is “and” then it should be all lowercase.

sample code for converting the data :

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

# Task 3: New State/UT formation
􏰀 In 2014 Telangana was formed after it split from Andhra Pradesh, The districts that were included in Telangana are stored in Data/Telangana.txt . Read the text file and Rename the State/UT From “Andhra Pradesh” to “Telangana” for the given districts.
􏰀 In 2019 Ladakh was formed after it split from Jammu and Kashmir, which included the districts Leh and Kargil. Rename the State/UT From “Jammu and Kashmir” to “Ladakh” for the given districts.

# Ladak formation
df['State/UT']=np.where(df['District']=="Leh(Ladakh)",'Ladakh',df['State/UT'])
df['State/UT']=np.where(df['District']=="Kargil",'Ladakh',df['State/UT'])
df.query('District=="Leh(Ladakh)" or District=="Kargil"')

# Telugana 

T_district=['Adilabad','Nizamabad','Karimnagar','Medak','Hyderabad','Rangareddy','Mahbubnagar','Nalgonda','Warangal','Khammam']
df['State/UT']=np.where(df['District'].isin(T_district),'Telangana',df['State/UT'])
df[df['District'].isin(T_district)]

Task 4: Find and process Missing Data
Find and store the percentage of data missing for the columns.
Some data can be found and filled in by using information from other cells. Try to find the correct data by using information from other cells and filling it in. Find and store the percentage of data missing for each column.

#Storing Missing Values
missing_values_before=df.isnull().mean().round(4) * 100
print(missing_values_before)
df['Population'].isnull().sum()
#pd.isnull(df['Population']).sum()

#Process the Missing Data
val=df['Male']+df['Female']
df['Population']=df['Population'].fillna(value=val)

l=['Literate_Male','Literate_Female']
l_value=df[l].sum(axis=1)
df['Literate']=df['Literate'].fillna(value=l_value)

h=['Households_Rural','Households_Urban']
h_value=df[h].sum(axis=1)
df['Households']=df['Households'].fillna(value=h_value)

# Task 5: Save Data to MongoDB
Save the processed data to mongoDB with a collection named “census” .

# Task 6: Database connection and data upload.
Data should be fetched from the mongoDB and to be uploaded to a relational database using python code . The table names should be the same as the file names without the extension.
The primary key and foreign key constraints should be included in the tables wherever required.

# Task 7: Run Query on the database and show output on streamlit

1. What is the total population of each district?
2. How many literate males and females are there in each district?
3. What is the percentage of workers(both male and female) in each district?
4. How many households have access to LPG or PNG as a cooking fuel in each
district?
5. What is the religious composition(Hindus,Muslims,Christians,etc.)of each
district?
6. How many households have internet access in each district?
7. What is the educational attainment distribution(below primary,primary,
middle, secondary, etc.) in each district?
8. How many households have access to various modes of transportation
(bicycle, car, radio, television, etc.) in each district?
9. What is the condition of occupied census houses(dilapidated,withseparate
kitchen, with bathing facility, with latrine facility, etc.) in each district?
10.How is the household size distributed (1 person, 2 persons, 3-5 persons, etc.)
in each district?
11. What is the total number of households in each state?
12.How many households have a latrine facility within the premises in each
state?
13.What is the average household size in each state?
14.How many households are owned versus rented in each state?
15.What is the distribution of different types of latrine facilities (pit latrine, flush
latrine, etc.) in each state?
16.How many households have access to drinking water sources near the
premises in each state?
17.What is the average household income distribution in each state based on the
power parity categories?
18.What is the percentage of married couples with different household sizes in
each state?
19.How many households fall below the poverty line in each state based on the
power parity categories?
20.What is the overall literacy rate (percentage of literate population) in each
state?

Write the SQL query for the above problems like 
1)query ='Select District,Population from census_2011'
t1 = pd.read_sql(query, engine)

update the code in the .py file and run it through the streamlit run P1_app.py in command promt.

   
