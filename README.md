Data Warehouse and Analytics Project 🚀

Welcome to the Data Warehouse and Analytics Project repository! This project demonstrates a comprehensive data warehousing and analytics solution, moving from raw data ingestion to generating actionable business insights. Designed as a portfolio piece, it highlights industry best practices in data engineering and analytics.



🏗 High-Level Architecture 

This project implements the Medallion Architecture, organizing data into three distinct layers to ensure quality and reliability:

Bronze Layer (Raw): Ingests raw data from source systems (ERP and CRM) provided as CSV files. This layer uses Batch Processing with a Truncate & Insert strategy, keeping the data "as-is" without any initial transformation.

Silver Layer (Cleansed): Focuses on Data Cleaning, Standardizing, and Normalizing. It resolves data quality issues and derives necessary columns to prepare the data for integration.

Gold Layer (Analytics): The final stage where data is integrated into a Star Schema. This layer supports Business Logic and Aggregations, making the data ready for reporting.



🎯 Project Requirements

👷🏽Building the Data Warehouse (Data Engineering)

Objective: Develop a modern data warehouse using MySQL to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications:

Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.

Data Quality: Cleanse and resolve data quality issues prior to analysis.

Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.

Scope: Focus on the latest dataset only; historization of data is not required.

Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.



📊BI: Analytics & Reporting (Data Analytics)

Objective: Develop SQL-based analytics to deliver detailed insights into:


Customer Behavior

Product Performance

Sales Trends


These insights empower stakeholders with key business metrics, enabling strategic decision-making.



🛠 Tech Stack

Database: MySQL 

ETL: SQL

Data Modeling: Star Schema (Fact & Dimension Tables) 

Visualization: Power BI / SQL Reporting 



📜 License

This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.



👨‍💻 About Me

Hi there! I'm Yeswanth Battina, I’m a Data-driven professional with a focus on turning complex datasets into actionable business insights.
Check out my profile to know more about my work and connect with me on (https://www.linkedin.com/in/yeswanth-battina-519622266/)
