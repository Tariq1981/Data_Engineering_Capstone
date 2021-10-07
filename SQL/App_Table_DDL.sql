create table App_Category
(
	Category_Id INT,
	Category_Desc text,
	primary key(Category_Id)
);

create table Currency_Type
(
	Currency_Type_Id INT,
	Currency_Type_Desc text,
	primary key(Currency_Type_Id)
);

create table Developer
(
	Developer_Id INT,
	Developer_Name text,
	Developer_Website text,
	Developer_Email text,
	primary key (Developer_Id)
);

create table Content_Rating
(
	Cont_Rating_Id INT,
	Cont_Rating_Desc text,
	primary key(Cont_Rating_Id)
);

create table App
(
	App_Id text,
	App_Name text,
	Category_Id INT,
	Rating FLOAT,
	Rating_Num INT,
	Minimum_Installs INT,
	Maximum_Installs INT,
	Is_Free Char(1),
	Price FLOAT,
	Currency_Type_Id INT,
	Size_In_MB FLOAT,
	Min_OS_Version text,
	Developer_Id INT,
	Release_Dt DATE,
	Last_Update_Dt DATE,
	Cont_Rating_Id INT,
	Privacy_Policy text,
	Is_Ad_Supported char(1),
	Is_In_App_Purchase char(1),
	Is_Editor_Choice char(1),
	Scrapped_Dttm TIMESTAMP(0),
	primary key(App_Id)
);

create table Permission_Type
(
	Permission_Type_Id INT,
	Permission_Type_Desc text,
	primary key(Permission_Type_Id)
);

create table "Permission"
(
	Permission_Id INT,
	Permisison_Desc text,
	Permission_Type_Id INT,
	primary key(Permission_Id)
);

create table App_Permission
(
	App_Id text,
	Permission_Id INT,
	primary key(App_Id,Permission_Id)
)
