# 📊 Salary Survey ETL Pipeline – Capstone Project

This project was completed as part of the **Data Engineering Bootcamp by AiDataYard**.  
It demonstrates the end-to-end process of building a **data engineering pipeline** — from data extraction to visualization — to analyze global salary survey data.

---

## 🚀 Project Overview
The goal of this project was to design and implement a **complete ETL (Extract, Transform, Load) pipeline** that processes salary survey data and delivers meaningful business insights through analytics dashboards.

### Key Objectives:
- Extract raw survey responses from CSV files  
- Clean, transform, and enrich the dataset  
- Load processed data into a cloud data warehouse  
- Build a **Star Schema** model for analytical queries  
- Visualize insights using **Power BI**

---

## 🛠️ Tools & Technologies
| Category | Tools |
|-----------|-------|
| **Programming & Query** | Python, PostgreSQL |
| **Cloud & Storage** | AWS S3, Amazon Redshift |
| **Data Modeling** | Star Schema |
| **Visualization** | Power BI|
| **Version Control** | Git & GitHub |

---

## ⚙️ ETL Pipeline Workflow

### 1. **Extract**
- Extracted survey response data from a **CSV file**.
- Parsed and validated data types using **Python (Pandas)**.

### 2. **Transform**
- Performed data cleaning (removed nulls, invalid salary entries, etc.).
- Applied transformations such as currency standardization and categorization.
- Created staging and fact/dimension tables for schema design.

### 3. **Load**
- Loaded transformed data into **Amazon Redshift** via **AWS S3**.
- Used SQL queries to create a **Star Schema** for efficient querying and analytics.

---

## 🧩 Data Modeling

The star schema consists of:
- **Fact Table:** Salary and compensation details  
- **Dimension Tables:** Employee demographics, job details, education, and country  

This design allows fast and flexible analytical queries in BI tools.

---

## 📈 Power BI Dashboard

The project includes a **Power BI dashboard** file:  
📁 `SalarySurveyDataDashboard.pbix`

### 🔹 Key Visuals Used:
- **KPI Cards:**  
  - Total Respondents  
  - Average Salary  
  - Highest Salary  
  - Total Countries  

- **Bar & Column Charts:**  
  - Average Salary by Job Title  
  - Average Salary by Education Level  
  - Salary Distribution by Experience Level  
  - Count of Respondents by Country  

- **Pie / Donut Charts:**  
  - Respondent Distribution by Gender  
  - Education Level Breakdown  

- **Table / Matrix Views:**  
  - Detailed salary insights by industry and seniority  

### 📊 Dashboard Highlights:
- Clean and professional layout using consistent colors and formatting  
- Filters added for **country, gender, education, and experience**  
- DAX measures exclude null and zero salary values for accurate insights  

---

## 🧠 Key Learnings
- Building a scalable ETL pipeline using modern data engineering tools  
- Data modeling with **Star Schema** for analytics  
- Integration between **AWS Redshift** and **Power BI**  

---

## 📂 Repository Structure

```text
├── data/
│   └── data.csv
│
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   └── load_to_redshift.py
│
├── dashboard/
│   └── SalarySurveyDataDashboard.pbix
│
├── workflow-diagram/
│   └── Salary Survey Data.pdf
│
└── README.md
