# Automated-E-commerce-Sales-Analysis-Pipeline-in-Airflow
This data engineering project focuses on automating the process of downloading and integrating incremental sales data from an API into a normalized PostgreSQL database, with daily update of an analytical sql view that showcases the profitability and popularity of different product categories.


## First step: 
### Database design. 
I connected to API and investigated what type of data are expected to be updated. 
Created two schema in PostgreSQL: "staging" and "mart". Schema "staging" includes table "user_order_log", which is made to be fit for daily update data file from API. This are raw and unproccesed data that need to be watched.
Schema "mart" is normalized database schema to store the collected data. It containes tables "city", "customer", "item", "sales" and "calendar".


## Second step: 
### Creating aggregated table. 
At this step I created a table called "customer_retention" to analyze the profitability and popularity of various product categories. On a weekly basis, this table tracks the number of purchases made by new customers, returning customers, and customers who requested refunds for different item categories in different cities. This table also shows revenue metrics for each customer type and in each product category by weeks. This aggregated table can provide valuable insights into customer behavior and sales performance of each item category, empowering informed decision-making for business strategies. 


## Third step: 
### Data updating Automation. 
In this step, an Apache Airflow DAG (Python script) is implemented to automate the data updating process. The DAG consists of several steps: connecting to the API to request a report generation, then uploading data to the "staging" schema, specifically the "user_order_inc" table. It further updates tables in schema "mart": "item", "city", "customer", and "sales". Finally, the "customer retention" table is updated.
This DAG is scheduled to run daily, ensuring the data is kept up-to-date.
It incorporates two layers of data quality assurance testing. The first layer ensures that the file is generated successfully, while the second layer verifies that the number of customers in the 'user_order_log' file is greater than three. These checks are critical: script will stop itself if the file is missing or it contains three or fewer customers. In such cases, an error notification will be sent via email.




