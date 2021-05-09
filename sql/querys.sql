SELECT air.name, air.municipality, cities.city, code.country_name, code.country_code
FROM airport_codes_dim AS air
INNER JOIN us_cities_demographics_dim AS cities ON UPPER(air.municipality) = UPPER(cities.city)
INNER JOIN global_land_temperatures_by_city_dim AS temp_city ON UPPER(temp_city.city) = UPPER(cities.city)
INNER JOIN country_code_and_name_dim AS code ON UPPER(code.country_name) = UPPER(temp_city.country)
INNER JOIN immigration_dim AS immi ON immi.i94cit = code.country_code
LIMIT 200;
