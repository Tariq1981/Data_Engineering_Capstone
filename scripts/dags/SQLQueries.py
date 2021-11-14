create_schema = "CREATE SCHEMA IF NOT EXISTS {};"

delete_table_sql = """
SET SEARCH_PATH TO {};
TRUNCATE {};
"""

create_app_fact_table = """
SET SEARCH_PATH TO {};
CREATE TABLE IF NOT EXISTS {} 
(
    Auto_App_Id INTEGER,
	Category_Id iNTEGER,
	Currency_Type_Id INTEGER,
	Developer_Id INTEGER,
	Release_Year INTEGER,
	Release_Month INTEGER,
	Cont_Rating_Id INTEGER,
	Permission_Type_Id INTEGER,
	Total_Num_Permissions BIGINT,
	Count_Of_Apps BIGINT,
	Average_Rating FLOAT,
	Total_Rating_Num FLOAT,
	Total_Installs BIGINT,
	Count_Of_Free BIGINT,
	Count_Of_Paid BIGINT,
	Total_Price FLOAT,
	Total_Size_In_MB FLOAT,
	Count_Ad_Supported BIGINT,
	Count_In_App_Purchase BIGINT,
	Count_Of_Editor_Choice BIGINT,
	primary key(Auto_App_Id)
);
"""

create_app_category_table = """
SET SEARCH_PATH TO {};
CREATE TABLE IF NOT EXISTS {} 
(
    Category_Id INTEGER,
    Category_Desc VARCHAR(300),
	primary key(Category_Id)
);
"""

create_currency_type_table = """
SET SEARCH_PATH TO {};
CREATE TABLE IF NOT EXISTS {} 
(
    Currency_Type_Id INTEGER,
	Currency_Type_Desc VARCHAR(10),
	primary key(Currency_Type_Id)
);
"""

create_developer_table = """
SET SEARCH_PATH TO {};
CREATE TABLE IF NOT EXISTS {} 
(
	Developer_Id INTEGER,
	Developer_Name VARCHAR(300),
	Developer_Website VARCHAR(300),
	Developer_Email VARCHAR(200),
	primary key (Developer_Id)
);
"""

create_content_rating_table = """
SET SEARCH_PATH TO {};
CREATE TABLE IF NOT EXISTS {} 
(
	Cont_Rating_Id INTEGER,
	Cont_Rating_Desc VARCHAR(200),
	primary key(Cont_Rating_Id)
);
"""

create_permission_type_table = """
SET SEARCH_PATH TO {};
CREATE TABLE IF NOT EXISTS {} 
(
	Permission_Type_Id INTEGER,
	Permission_Type_Desc VARCHAR(300),
	primary key(Permission_Type_Id)
);
"""

check_table_count_only = """
SET SEARCH_PATH TO {};
SELECT COUNT(*) row_count
FROM {};
"""

check_table_count_duplicates = """
SET SEARCH_PATH TO {};
SELECT row_count, 
       CASE 
            WHEN cnt_dups > 0 THEN 0
            ELSE 1
       END dup_flg
FROM
(
    SELECT COUNT(*) row_count
    FROM {}
) cnt
CROSS JOIN
(
    SELECT COUNT(*) cnt_dups
    FROM
    (
        SELECT {},count(*) cnt
        FROM {}
        GROUP BY {}   
        HAVING count(*) > 1
    ) X
) dups; 
"""
