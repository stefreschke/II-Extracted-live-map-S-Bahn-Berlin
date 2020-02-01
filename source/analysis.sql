Select AVG(delay) From days;
/* Kann nicht stimmen, wieso negativ?*/


Select Count(*) From delays;
Select AVG(delay) From delays;
Select Max(delay), Min(delay) From delays;
Select Min(date), Max(date) From snapshot;
Select delay, Count(*) From delays Group by delay;
/* Überblick über Daten - Verspätungen */

Select Min(temperature), Max(temperature) From weather;
Select AVG(temperature) From weather;
Select AVG(temperature) From weather Where at_time Like '%07:00:00';
Select AVG(temperature) From weather Where at_time Like '%03:00:00'

Select round(temperature), Count(*) From weather Where at_time Like '%07:00:00' Group by round(temperature);
Select round(temperature), Count(*) From weather Where at_time Like '%07:00:00' Group by round(temperature);
Select condition, Count(*) From weather Group by condition;
/* Überblick über Daten - Wetter */



Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 07:00:00') Where weather.condition = 'clear';
Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 07:00:00') Where weather.condition = 'cloudy';
Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 07:00:00') Where weather.condition = 'rainy';
/* Scheint zu korrelieren, clear > cloudy > rainy */

Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 07:00:00') Where weather.temperature > 15;
Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 07:00:00') Where weather.temperature < 10;
Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 07:00:00') Where weather.temperature >= 10 And weather.temperature <= 15;
/* Keine direkte Korrelation, 10-15 Grad höhere Verspätung*/

Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 03:00:00') Where weather.temperature > 10;
Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 03:00:00') Where weather.temperature < 5;
Select AVG(delay), Count(*) From delays Join snapshot On snapshot.id = delays.snapshot Join weather on weather.at_time = (snapshot.date || ' 03:00:00') Where weather.temperature >= 5 And weather.temperature <= 10;
/* Keine direkte Korrelation, 5-10 Grad höhere Verspätung*/