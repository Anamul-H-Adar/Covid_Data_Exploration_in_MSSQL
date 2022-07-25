
-- Explore our dataset which are we import
SELECT * 
FROM CovidProject..CovidDeath
WHERE continent IS NOT NULL
ORDER BY location, date;

-- Select a particular some column to explore for furthur calculation
SELECT location, date, population, total_cases, new_cases, total_deaths
FROM CovidProject..CovidDeath
WHERE continent IS NOT NULL
ORDER BY 1, 2;

-- Perform a simple calculation
-- find out total_cases VS total_deaths
SELECT location, date, population, total_cases, new_cases, total_deaths,
ROUND((total_deaths/total_cases)*100,2) as totalDeathPercentage
FROM CovidProject..CovidDeath
WHERE location = 'Bangladesh'
AND continent IS NOT NULL
ORDER BY 1, 2;

-- Perform a simple calculation
-- find out total cases percentage against the population
SELECT location, date, population, total_cases, ROUND((total_cases/population)*100,2) as popIncPercentage -- Population percentage Infection
FROM CovidProject..CovidDeath
WHERE location = 'Bangladesh'
AND continent IS NOT NULL
ORDER BY 1, 2;

-- Find out country wise highest infection rate compare to population
SELECT location, MAX(population) highest_population, MAX(total_cases) highest_infection,
ROUND(MAX(total_cases/population)*100,2) as popIncPercentage -- Population percentage Infection
FROM CovidProject..CovidDeath
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY popIncPercentage DESC;

-- Find out locaiton wise maximum death records (Using Cast func because total death column datatype is nvarchar)
SELECT location, MAX(CAST(total_deaths as INT)) highest_deaths
FROM CovidProject..CovidDeath
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY highest_deaths DESC;

-- Find out country wise highest death rate compare to population
-- (Using Cast func because total death column datatype is nvarchar)
SELECT location, MAX(CAST(total_deaths as INT)) highest_deaths,
ROUND(MAX(CAST(total_deaths as INT)/population) * 100,2) as MaxDeathRate
FROM CovidProject..CovidDeath
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY MaxDeathRate DESC;

-- Find out continent wise highest death rate compare to population
SELECT continent, MAX(CAST(total_deaths as INT)) highest_deaths,
ROUND(MAX(CAST(total_deaths as INT)/population) * 100,2) as MaxDeathRate
FROM CovidProject..CovidDeath
--WHERE continent IS NULL
GROUP BY continent
ORDER BY MaxDeathRate DESC;

-- Find out country wise yearly highest death rate compare to population
-- and yearly highest total_cases rate compare to population
SELECT location, YEAR(date) case_year, MAX(CAST(total_deaths as INT)) highest_deaths, MAX(total_cases) highest_infection,
ROUND(MAX(CAST(total_cases as INT)/population) * 100,2) as MaxInfectedRate,
ROUND(MAX(CAST(total_deaths as INT)/population) * 100,2) as MaxDeathRate
FROM CovidProject..CovidDeath
WHERE continent IS NOT NULL
GROUP BY location,  YEAR(date) 
ORDER BY MaxDeathRate, MaxInfectedRate DESC;

-- Find out yearly total cases and total deaths with total death percentage compare to total cases
SELECT YEAR(date) covid_year, MONTH(date) covid_month, SUM(total_cases) total_cases, SUM(CAST(total_deaths as INT)) total_deaths,
ROUND(SUM(CAST(total_deaths as INT))/ SUM(total_cases)*100,2) total_deaths_pecentage
FROM CovidProject..CovidDeath
WHERE continent IS NOT NULL
GROUP BY YEAR(date), MONTH(date)
ORDER BY covid_year, covid_month;

-- Find out vaccination vs population
SELECT de.continent, de.location, va.date, de.population, CAST(va.total_vaccinations as bigint) total_vaccinations
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON de.location = va.location
  AND de.date = va.date
WHERE de.continent IS NOT NULL
ORDER BY de.location, va.date;

-- Find out monthly total case, total deaths and total vaccination
-- total vaccination column cumulatively inserted data into table that's why Maximum number is total vaccination number
SELECT de.continent, de.location, YEAR(va.date) Covid_Year,MONTH(va.date) Covid_Month,
SUM(de.total_cases) total_cases, SUM(CAST(de.total_deaths as INT)) total_deaths, 
MAX(CAST(va.total_vaccinations as bigint)) total_vaccinations
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON de.location = va.location
  AND de.date = va.date
WHERE de.continent IS NOT NULL
GROUP BY de.continent, de.location, YEAR(va.date) ,MONTH(va.date)
ORDER BY de.location,Covid_Year, Covid_Month;


-- Find out monthly total vaccination rate compare to total cases
-- total vaccination column cumulatively inserted data into table that's why Maximum number is total vaccination number
SELECT de.continent, de.location, YEAR(va.date) Covid_Year,MONTH(va.date) Covid_Month,
SUM(de.total_cases) total_cases, ISNULL(MAX(CAST(va.total_vaccinations as bigint)),0) Total_vaccinations,
ISNULL(MAX(CAST(va.total_vaccinations as bigint)),0)/ SUM(de.total_cases)*100 Vaccination_rate
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON  de.location = va.location
  AND de.date     = va.date
WHERE de.continent IS NOT NULL
GROUP BY de.continent, de.location, YEAR(va.date) ,MONTH(va.date)
ORDER BY de.location,Covid_Year, Covid_Month;


UPDATE CovidProject..CovidVaccination
SET new_vaccinations = '0'
WHERE new_vaccinations IS NULL;


-- Find out total population vs total vaccination
SELECT de.continent, de.location, de.date, de.population, va.new_vaccinations,
SUM(CAST(va.new_vaccinations as BIGINT)) OVER(PARTITION BY de.location ORDER BY de.location ,de.date) CumulativeTotalVaccination
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON  de.location = va.location
  AND de.date     = va.date
WHERE de.continent IS NOT NULL
GROUP BY de.continent, de.location, de.date, de.population, va.new_vaccinations
ORDER BY  de.location, de.date;

-- Find out total CumulativeTotalVaccination percentage rate compare to total population
-- Use CTE to devide CumulativeTotalVaccination/ population
WITH vaccPercentage AS
(
SELECT de.continent, de.location, de.date, de.population, va.new_vaccinations,
SUM(CAST(va.new_vaccinations as BIGINT)) OVER(PARTITION BY de.location ORDER BY de.location ,de.date) CumulativeTotalVaccination
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON  de.location = va.location
  AND de.date     = va.date
WHERE de.continent IS NOT NULL
GROUP BY de.continent, de.location, de.date, de.population, va.new_vaccinations
)
SELECT continent, location, date, population, new_vaccinations,CumulativeTotalVaccination,
(CumulativeTotalVaccination/population)*100 CumVacPercentage -- CumVacPercentage = Cumulative Vaccination Percentage
FROM vaccPercentage;

----
-- Find out total CumulativeTotalVaccination percentage rate compare to total population
-- Use temp table to devide CumulativeTotalVaccination/ population

CREATE TABLE #cumVaccination --- Creating temp table named as #cumVaccination
(
 continent					NVARCHAR(100),
 location					NVARCHAR(100),
 date						DATETIME,
 population					NUMERIC,
 new_vaccinations			NUMERIC,
 CumulativeTotalVaccination NUMERIC,
);

INSERT INTO #cumVaccination
SELECT de.continent, de.location, de.date, de.population, va.new_vaccinations,
SUM(CAST(va.new_vaccinations as BIGINT)) OVER(PARTITION BY de.location ORDER BY de.location ,de.date) CumulativeTotalVaccination
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON  de.location = va.location
  AND de.date     = va.date
WHERE de.continent IS NOT NULL
GROUP BY de.continent, de.location, de.date, de.population, va.new_vaccinations

SELECT continent, location, date, population, new_vaccinations,CumulativeTotalVaccination,
      (CumulativeTotalVaccination/population) CumVacPercentage
FROM #cumVaccination;

-- Creating a VIEW to calculate Cumulative New Vaccition rate compare to population Using temp table
CREATE VIEW vcumVaccination 
AS
SELECT de.continent, de.location, de.date, de.population, va.new_vaccinations,
SUM(CAST(va.new_vaccinations as BIGINT)) OVER(PARTITION BY de.location ORDER BY de.location ,de.date) CumulativeTotalVaccination
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON  de.location = va.location
  AND de.date     = va.date
WHERE de.continent IS NOT NULL
GROUP BY de.continent, de.location, de.date, de.population, va.new_vaccinations

select * from vcumVaccination


SELECT de.*, va.*
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON  de.location = va.location
  AND de.date     = va.date
WHERE de.continent IS NOT NULL

-- Find out aged people over 65 and 70 years old and their diabetics prevalence
SELECT de.continent,de.location,de.population, MAX(va.male_smokers) HighestPercentagamalesmokers,
MAX(va.female_smokers) HighestPercentagafemalesmokers
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON  de.location = va.location
  AND de.date     = va.date
WHERE de.continent IS NOT NULL
GROUP BY de.continent,de.location,de.population
ORDER BY de.population,HighestPercentagamalesmokers, HighestPercentagafemalesmokers DESC;

-- Total Population shown null at Northern Cyprus that's why we filter out that out script is accurate or not
-- There is no population in location Northern Cyprus
SELECT SUM(population) total_population
FROM CovidProject..CovidVaccination
WHERE location = 'Northern Cyprus'

-- Find out Highest Smoker Male percentage vs Female percentage compare to population
SELECT de.continent,de.location,de.population, MAX(va.aged_65_older) HighestPercentaga65aged,
MAX(va.aged_70_older) HighestPercentaga70aged
FROM CovidProject..CovidDeath de
JOIN CovidProject..CovidVaccination va
  ON  de.location = va.location
  AND de.date     = va.date
WHERE de.continent IS NOT NULL
GROUP BY de.continent,de.location,de.population
ORDER BY de.population,HighestPercentaga70aged, HighestPercentaga65aged DESC;