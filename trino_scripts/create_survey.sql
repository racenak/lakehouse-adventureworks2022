CREATE TABLE delta.silver.person_demographics (
	businessentityid integer,
    TotalPurchaseYTD double,
    DateFirstPurchase varchar,
    BirthDate varchar,
    MaritalStatus varchar,
    YearlyIncome varchar,
    Gender varchar,
    TotalChildren varchar,
    NumberChildrenAtHome varchar,
    Education varchar,
    Occupation varchar,
    HomeOwnerFlag varchar,
    NumberCarsOwned varchar,
    CommuteDistance varchar
)
WITH (
    location = 's3a://lake/silver/person_demographics'
);