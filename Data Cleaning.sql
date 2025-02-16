-- DATA CLEANING

-- CREATE TABLE:

DROP TABLE IF EXISTS layoffs;
CREATE TABLE layoffs 
	(
	company VARCHAR (30),
	location VARCHAR (30),	
	industry VARCHAR (30),
	total_laid_off INT,
	percentage_laid_off	FLOAT,
	layoff_date	VARCHAR (30),
	stage VARCHAR (30),
	country VARCHAR (30),
	funds_raised_millions FLOAT
	)

-- CREATE DUPLICATED TABLE TO PERFORM CRUD OPERATIONS

SELECT * INTO layoff_staging
FROM layoffs;

-- STEPS & REQUIREMENTS

-- Step 1) Check for Duplicate Values

SELECT *
FROM (
	SELECT *,
	ROW_NUMBER () OVER (
	PARTITION BY company, location, industry, total_laid_off,percentage_laid_off, 
	layoff_date, stage, country, funds_raised_millions) row_num
	FROM layoff_staging
	)
WHERE row_num > 1;

-- DELETE DUPLICATE ROWS

WITH duplicate_check AS (
    SELECT ctid
    FROM (
        SELECT ctid, 
               ROW_NUMBER() OVER (
                   PARTITION BY company, location, industry, total_laid_off, 
                   percentage_laid_off, layoff_date, stage, country, funds_raised_millions
               ) AS row_num
        FROM layoff_staging
    ) subquery
    WHERE row_num > 1
)
DELETE FROM layoff_staging
WHERE ctid IN (SELECT ctid FROM duplicate_check);

-- Standardizing DATA

SELECT DISTINCT(TRIM(company)),
	company
FROM layoff_staging;

UPDATE layoff_staging
SET company = TRIM(company);

/* Update industry column for incosistencies in the values  
*/

SELECT DISTINCT industry
FROM layoff_staging
ORDER BY 1;

UPDATE layoff_staging
SET industry = 'Crypto'
WHERE  industry LIKE 'Crypto%';

/* Update country column for incosistencies in the values  
*/

SELECT DISTINCT country
FROM layoff_staging
ORDER BY 1;

UPDATE layoff_staging
SET country = 'United States'
WHERE country LIKE 'United States%';

/* Updating NULL values in industry columns with the Correct Values
*/

-- Checking for the NULL rows

SELECT *
FROM layoff_staging t1
JOIN layoff_staging t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL) AND t2.industry IS NOT NULL

-- Updating the NULL rows with values
	
UPDATE layoff_staging t1
SET industry = t2.industry
FROM layoff_staging t2
WHERE t1.company = t2.company
  AND (t1.industry IS NULL OR t1.industry = ' ')
  AND t2.industry IS NOT NULL;


SELECT *
	FROM layoff_staging
WHERE company LIKE 'Bally%'

UPDATE layoff_staging
SET industry = NULL
WHERE company LIKE 'Bally%'

/* DELETING rows with total_laid_off and percentage_laid_off AS BLANK or NULLS 
*/


DELETE 
FROM layoff_staging
WHERE total_laid_off IS NULL 
  AND percentage_laid_off IS NULL


SELECT * 
FROM layoff_staging
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL


/* Update date column to change values from string format to date format
*/

ALTER TABLE layoff_staging
ALTER COLUMN layoff_date TYPE DATE
USING TO_DATE(layoff_date, 'MM/DD/YYYY');

SELECT 
    layoff_date,
    TO_DATE(layoff_date, 'MM/DD/YYYY') AS formatted_layoff_date
FROM 
    layoff_staging;

UPDATE layoff_staging
SET layoff_date = TO_DATE(layoff_date, 'MM/DD/YYYY') 


-- EXPLORATORY ANALYSIS ON THE CLEANED DATA






