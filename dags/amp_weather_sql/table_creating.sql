create table if not exists amplitude_weather (
	amp_id serial primary key,
	city_name varchar(50),
	temperature float,
	pressure float,
	humidity float,
	timestamp timestamp	
)
