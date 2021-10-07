create table Country
(
	Country_Code text,
	Country_Name text,
	Continent text,
	Region text,
	Iso_region text, 
	primary Key(Country_Code)
);

create table City
(
   City_Id INT,
   City_Name text,
   Country_Code text,
   Latitude FLOAT,
   Longitude Float,
   primary Key(City_ID)
);

create table US_State
(
	State_Code text,
	State_Name text,
	primary Key(State_Code)
);
create table US_City_Demog
(
	City_Id INT,
	State_Code text,
	Median_Age FLOAT,
	Male_Population FLOAT,
	Female_Population INT,
	Unknown_Population INT,--- Male+Female subtracted from total if zeo
	Num_of_Veterans INT,
	Foreign_Born INT,
	Avg_Household_Size FLOAT,
	Race text,
	Race_Count INT,
	primary key(City_Id)
	
)


create table City_Temprature_Hist
(
	City_Id INT,
	"Year" INT,
	"Month" INT,
	AvgTemperature Float,
	AvgTemperatureUncertainty FLOAT,
	primary key(City_Id,"Year","Month")
);

create table Airport_Type
(
	Airport_Type_Id INT,
	Airport_Type_Desc text,
	primary key(Airport_Type_Id)
);

create table Airport
(
	Airport_Id text,
	Airport_Name text,
	Airport_Type_Id INT,
	Elevation_ft FLOAT,
	Country_Code text,
	Latitude FLOAT,
	Logitude FLOAT,
	primary key(Airport_Id)
);

create table Travel_Medium
(
	Travel_Medium_Id INT,
	Travel_Medium_Desc text,
	primary key(Travel_Medium_Id)
)

create table Visa_Type
(
	Visa_Type_Id INT,
	Visa_Type_Desc text,
	primary key(Visa_Type_Id)
)

create table NonImmigrantTemp_Type
(
	NonImmigTemp_Visa_Type_Code text,
	NonImmigTemp_Visa_Type_Desc text,
	primary key(NonImmigTemp_Visa_Type_Code)
)

create table Immigration_Trx
(
	Immig_Trx_Id text,
	"Year" INT,
	"Month" INT,
	Citizen_Country_Code text,--I94CIT 
	Airport_Id text,
	Arrival_Dt Date,
	Travel_Medium_Id INT,
	US_State_Code text,
	Departure_Dt Date,
	Immig_Age INT,
	Visa_Type_Id INT,
	Write_Trns_Dt DATE,
	Issu_Visa_Dept_Stat text,
	Occupation text,
	Arrival_Signal_Flg char(1),
	Departure_Signal_Flg char(1),
	Update_Signal_Flg char(1),
	Match_Flg char(1),
	Birth_Year INT,
	Allowed_Stay_Dt DATE,
	Gender char(1),
	Ins_Number INT,
	Airline_Code text,
	admisison_Number float,
	FLight_Number text,
	NonImmigTemp_Visa_Type_Code text,
	primary key(Immig_Trx_Id)
)