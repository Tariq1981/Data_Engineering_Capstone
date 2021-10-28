import configparser
config = configparser.ConfigParser()
config.read('../config/etl.cfg')

create_schema = "CREATE SCHEMA IF NOT EXISTS MODEL;"
drop_table_sql = """
SET SEARCH_PATH TO MODEL;
DROP TABLE IF EXISTS {};
"""

create_app_fact_table = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE {} IF NOT EXISTS 
(
    Auto_App_Id INTEGER,
	Category_Id iNTEGER,
	Currency_Type_Id INTEGER,
	Developer_Id INTEGER,
	Release_Year INTEGER,
	Release_Month INTEGER,
	Cont_Rating_Id INTEGER,
	Permission_Type_Id INTEGER,
	Total_Num_Permissions INTEGER,
	Count_Of_Apps INTEGER,
	Average_Rating FLOAT,
	Total_Rating_Num FLOAT,
	Total_Installs INTEGER,
	Count_Of_Free INTEGER,
	Count_Of_Paid INTEGER,
	Total_Price FLOAT,
	Total_Size_In_MB FLOAT,
	Count_Ad_Supported INTEGER,
	Count_In_App_Purchase INTEGER,
	Count_Of_Editor_Choice INTEGER,
	primary key(Auto_App_Id)
);
""").format(config["DWH_TABLES"]["APP_FACT_FT"])

create_app_category_table = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE {} IF NOT EXISTS 
(
	Category_Id INTEGER,
	Category_Desc VARCHAR(300),
	primary key(Category_Id)
);
""").format(config["DWH_TABLES"]["APP_CATEGORY_DM"])

create_currency_type_table = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE {} IF NOT EXISTS 
(
	Currency_Type_Id INTEGER,
	Currency_Type_Desc VARCHAR(10),
	primary key(Currency_Type_Id)
);
""").format(config["DWH_TABLES"]["CURRENCY_TYPE_DM"])

create_developer_table = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE {} IF NOT EXISTS 
(
	Developer_Id INTEGER,
	Developer_Name VARCHAR(300),
	Developer_Website VARCHAR(300),
	Developer_Email VARCHAR(200),
	primary key (Developer_Id)
);
""").format(config["DWH_TABLES"]["DEVELOPER_DM"])

create_content_rating_table = ("""
SET SEARCH_PATH TO MODEL;
CREATE TABLE {} IF NOT EXISTS 
(
	Cont_Rating_Id INTEGER,
	Cont_Rating_Desc VARCHAR(200),
	primary key(Cont_Rating_Id)
);
""").format(config["DWH_TABLES"]["CONTENT_RATING_DM"])
