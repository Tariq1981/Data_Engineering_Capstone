create table App_Fact
(
	Auto_App_Id INT, -- generated
	Category_Id INT,
	Currency_Type_Id INT,
	Developer_Id INT,
	Release_Year INT,
	Release_Month INT,
	Cont_Rating_Id INT,
	Permission_Type_Id INT,
	Total_Num_Permissions INT,
	Count_Of_Apps INT,
	Average_Rating FLOAT,
	Total_Rating_Num Float,
	Total_Installs INT,
	Count_Of_Free INT,
	Count_Of_Paid INT,
	Total_Price FLOAT,
	Total_Size_In_MB FLOAT,
	Count_Ad_Supported INT,
	Count_In_App_Purchase INT,
	Count_Of_Editor_Choice INT,
	primary key(Auto_App_Id)
);

