--Access appropriate database
USE CovidData;


--Initial look at some of the data
---Select, From, and Order by clauses
SELECT location, date, new_cases, total_cases, total_deaths, population
FROM mortality
ORDER BY location, date;

SELECT location, date, new_tests, new_vaccinations, people_vaccinated, people_fully_vaccinated
FROM vaccinations
ORDER BY location, date;


--Up-to-date case and death numbers by geographic location
---Aggregate functions, Where, Like, and Group by clauses
SELECT location, MAX(total_cases) AS cases, MAX(total_deaths) AS deaths
FROM mortality
WHERE total_deaths IS NOT NULL
AND location NOT LIKE '%income'
GROUP BY location
ORDER BY deaths DESC;


--Deaths/Population by country
---Casting, Aliases, Telling a story with data
SELECT continent,
	location,
	population,
	MAX(total_deaths) as total_deaths,
	(CAST(MAX(total_deaths) AS decimal)/population)*100 AS '% population removed by COVID',
	CAST((1/(CAST(MAX(total_deaths) AS decimal)/population)) AS bigint) AS '1 out of _ people in this group'
FROM mortality
GROUP BY continent, location, population
ORDER BY '% population removed by COVID' DESC;


--Impact of income brackets on infection and mortality rates
---Concatenation, Order by Case clause
SELECT location AS income_bracket,
	population,
	MAX(total_cases) as cases,
	MAX(total_deaths) as deaths,
	(CAST(MAX(total_deaths) AS decimal)/MAX(total_cases))*100 AS 'infected mortality %',
	(CAST(MAX(total_cases) AS decimal)/population)*100 AS 'total illness %',
	(CAST(MAX(total_deaths) AS decimal)/population)*100 AS 'total mortality %',
	CONCAT('1 out of ', CAST(1/(CAST(MAX(total_deaths) AS decimal)/population) AS int), ' people in this bracket') AS 'killed by COVID'
FROM mortality
WHERE location LIKE '%income'
GROUP BY location, population
ORDER BY CASE location
	WHEN 'Low income' THEN 1
	WHEN 'Lower middle income' THEN 2
	WHEN 'Upper middle income' THEN 3
	WHEN 'High income' THEN 4
   END;



--Data integrity: Comparing total_vaccinations column to calculated total vaccinations
---Common Table Expression, Window function for rolling sum
WITH calculated_vaccs AS 
(
	SELECT location,
		date,
		new_vaccinations,
		SUM(CAST(new_vaccinations AS bigint)) OVER (PARTITION BY location ORDER BY date) AS sum_of_new_vaccs,
		total_vaccinations
	FROM vaccinations
	WHERE new_vaccinations IS NOT NULL
	AND total_vaccinations IS NOT NULL
	AND total_vaccinations != 0
)
SELECT *,
	CAST(sum_of_new_vaccs AS decimal)/total_vaccinations AS similarity
FROM calculated_vaccs
ORDER BY location, date;


--Data Integrity: Comparing total_tests column to calculated total tests
---Temp Table
DROP TABLE IF EXISTS #calculated_tests
CREATE TABLE #calculated_tests
(
location nvarchar (50),
date date,
new_tests int,
sum_of_new_tests bigint,
total_tests bigint
);

INSERT INTO #calculated_tests
SELECT location,
	date,
	new_tests,
	SUM(CAST(new_tests AS bigint)) OVER (PARTITION BY location ORDER BY date) AS sum_of_new_tests,
	total_tests
FROM vaccinations
WHERE new_tests IS NOT NULL
AND total_tests IS NOT NULL
AND total_tests != 0
ORDER BY location, date;

SELECT *,
	CAST(sum_of_new_tests AS decimal)/total_tests AS similarity
FROM #calculated_tests
ORDER BY location, date;


--Has the mortality rate decreased over time (treatments, etc)?
SELECT date, 
	AVG((CAST(total_deaths AS decimal)/total_cases)*100) AS mortality_rate
FROM mortality
WHERE total_deaths IS NOT NULL
GROUP BY date
ORDER BY date;


--Identifying the most recent available mortality rate for each location
--[Reveals possible trend of absent data in locations with a non-current MAX(date); explored below]
SELECT location, 
	   date AS 'date of most recent data', 
	  (CAST(total_deaths AS decimal)/total_cases)*100 AS mortality_rate
FROM mortality m
WHERE date =  
(
	SELECT MAX(date) FROM mortality
	WHERE m.location = location
)
ORDER BY location, date;


--Confirming general absence of data in locations with an earlier MAX(date)
SELECT * FROM mortality m
WHERE   
	(
	SELECT MAX(date) FROM mortality
	WHERE m.location = location
	) != 
	(
	SELECT MAX(date) FROM mortality
	)
ORDER BY iso_code;


--Clearly identifying locations for which other data sources must be used
SELECT DISTINCT continent, location
FROM mortality m
WHERE   
	(
	SELECT MAX(date) FROM mortality
	WHERE m.location = location
	) !=
	(
	SELECT MAX(date) FROM mortality
	)
ORDER BY continent, location;


--Strictness of government response vs transmission
---Rounding to match level of precision in columns, removing trailing zeros
SELECT v.stringency_index AS strictness, 
	CAST(ROUND(AVG(m.new_cases_per_million),2) AS float) AS new_cases_per_million
FROM mortality m
JOIN vaccinations v
ON m.iso_code = v.iso_code
AND m.date = v.date
WHERE m.new_cases_per_million != 0
AND v.stringency_index IS NOT NULL
GROUP BY v.stringency_index
ORDER BY v.stringency_index DESC;


--How did Sweden's more relaxed approach affect infection rate?
---solved using Subqueries, Union with totals
SELECT DISTINCT 'new' as cases,
	m.date,
	(
	SELECT new_cases_per_million FROM mortality
	WHERE location = 'Sweden'
	AND m.date = date
	) AS 'swedish cases per million',
	(
	SELECT new_cases_per_million FROM mortality
	WHERE location = 'World'
	AND m.date = date
	) AS 'global cases per million'
FROM mortality m
UNION
SELECT 'total' as cases,
	NULL,
	(
	SELECT MAX(total_cases_per_million) FROM mortality
	WHERE location = 'SWEDEN'
	),
	(
	SELECT MAX(total_cases_per_million) FROM mortality
	WHERE location = 'World'
	)
FROM mortality
ORDER BY cases, date;


--How did China's more strict approach affect infection rates?
---solved using Joins
SELECT DISTINCT 'new' AS cases,
	c.date, 
	c.new_cases_per_million AS 'chinese cases per million',
	w.new_cases_per_million AS 'global cases per million'
FROM mortality c
JOIN mortality w
ON c.date = w.date
WHERE c.location = 'China' 
AND w.location = 'World'
UNION
SELECT 'total' AS cases,
	NULL,
	MAX(c.total_cases_per_million),
	MAX(w.total_cases_per_million)
FROM mortality c
INNER JOIN mortality w
ON c.date = w.date
WHERE c.location = 'China'
AND w.location = 'World'
ORDER BY cases, c.date;


--Conclusion of preceding queries, more concisely
SELECT 'cases per million' AS ' ',
	CAST(MAX(s.total_cases_per_million) AS int) AS sweden,
	CAST(MAX(g.total_cases_per_million) AS int) AS world,
	CAST(MAX(c.total_cases_per_million) AS int) AS china
FROM mortality s
JOIN mortality g ON s.date = g.date
JOIN mortality c ON s.date = c.date
WHERE s.location = 'Sweden'
AND g.location = 'World'
AND c.location = 'China'


--Vaccinations vs new cases
SELECT v.location,
	v.date,
	v.total_vaccinations,
	v.people_vaccinated,
	v.people_fully_vaccinated,
	(CAST(v.people_fully_vaccinated AS decimal)/m.population)*100 AS '% global population fully vaccinated',
	m.new_cases
FROM vaccinations v
JOIN mortality m
ON v.iso_code = m.iso_code
AND v.date = m.date
WHERE v.location = 'World'
ORDER BY v.date;


--Excess mortality by location
SELECT continent,
	location,
	date,
	excess_mortality_cumulative AS 'excess_mortality_%',
	excess_mortality_cumulative_absolute AS excess_mortality_#
FROM vaccinations
WHERE excess_mortality_cumulative IS NOT NULL
ORDER BY location, date;


 --Total excess lives lost per location
WITH final_report_dates (date, location) AS
(
SELECT MAX(date), location
FROM vaccinations
WHERE excess_mortality_cumulative_absolute IS NOT NULL
GROUP BY location
)
SELECT frp.location,
	frp.date AS date_reported,
	v.excess_mortality_cumulative AS 'excess_mortality_%',
	CAST(v.excess_mortality_cumulative_absolute AS int) AS excess_lives
FROM final_report_dates frp
JOIN vaccinations v
ON frp.date = v.date AND frp.location = v.location
ORDER BY excess_lives DESC



--Deadliest day of the pandemic
---Select TOP clause, Excluding recent data where this statistic was sometimes aggregated
SELECT TOP 1
	location,
	date,
	new_deaths
FROM mortality
WHERE date BETWEEN '2020-01-01' AND '2021-12-31'
ORDER BY new_deaths DESC;


--Recent daily impact of pandemic
SELECT TOP 28
	location,
	date,
	new_cases,
	new_deaths,
	total_deaths
FROM mortality
ORDER BY total_deaths DESC;