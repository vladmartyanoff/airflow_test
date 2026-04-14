with ranked_temps as 	(SELECT 
    					city_name, temperature, pressure, humidity, timestamp, ROW_NUMBER() OVER (PARTITION BY city_name, DATE(timestamp) ORDER BY temperature DESC, timestamp DESC) as rn
  						FROM city_weather_data 
  						WHERE DATE(timestamp) = CURRENT_DATE)
  						
insert into amplitude_weather (city_name, temperature, pressure, humidity, timestamp)
select city_name, temperature, pressure, humidity, timestamp
from ranked_temps
where rn = 1
ON CONFLICT (amp_id) DO NOTHING

  
		

