create schema delta.bronze 
WITH (location = 's3a://lake/bronze');

create schema delta.silver 
WITH (location = 's3a://lake/silver');

create schema delta.gold
with (location = 's3a://lake/gold');

create schema delta.mart
with (location = 's3a://lake/mart');
