# Databricks_Finance_analysis

Instructions:
1. Please first upload the Raw_finance file into your DataBricks
2. import the the Finance_analysis.ipynb file in to your workspace 
3. Give the source path file into the Finance_analysis file where we have uploaded_parh=""


Problem statement:
1. Rename the "industry_code_ANZSIC" and "industry_name_ANZSIC" with "industry_code" and "industry_name” respectively
2. Create a new column called "Amount_in_Dollers" using the "unit" column where if the unit value is equal to "DOLLARS(millions)" 
then multiply the "value" column with 10000000 else assign it as 0
3. Create a new column called "Amount_in_Rupees" using the "unit" column where if the unit value is equal to "COUNT" 
then multiply the "value" column with 10000000 else assign it as 0
4. Remove the columns "value", "unit", and "rme_size_grp" from the data frame and save the new data frame to a new path 
5. Use the new parth data and print the Industry max amount in dollars and rupees 
6. Find the rolling sum of dollars and Rupees based on years
7. Identifying the Even rows and finding Rolling Sum based on distinct Industry
8. Identifying the max value of Amount_in_Rupees and display the year and industry name 
