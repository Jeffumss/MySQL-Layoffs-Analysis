-- Data Cleaning Portion
-- STEPS TO CLEANING MY DATA
-- 1. Remove Duplicates
-- 2. Standardize the Data/Cleaning Columns
-- 3. Fix Null Values or Blank Values
-- 4. Remove Any Columns Not Required for Analysis

USE world_layoffs;

-- Viewing oringal database table.
SELECT *
FROM layoffs;


-- Duplicating my original table to make changes to staging table.
DROP TABLE IF EXISTS layoffs_staging;
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Inserting into the new staging table a copy of the data from the original table.
INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;


-- 1. Removing Duplicates without an unquie identifier.

-- Using a cte to assign a row number to each row partitioned by each of the columns together. 
-- Querying for row number 2 of exact duplicate rows.
WITH duplicate_cte AS 
(
 SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, 
		total_laid_off, percentage_laid_off,`date`, 
		stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging
) 
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;
-- There are 5 duplicate rows.


-- Creating a new staging table duplicate with a new column for row_num.
DROP TABLE IF EXISTS layoffs_staging2;
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Insering my original data into the new stagging table.
INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, 
    industry, total_laid_off, percentage_laid_off,`date`, 
    stage, country, funds_raised_millions) AS row_num 
FROM layoffs_staging;

-- Selecting the rows of data that are duplicates.
SELECT * 
FROM layoffs_staging2
WHERE row_num >1;

-- Deleting the duplicate rows (row_num greater than 1) from the duplicate staging table.
DELETE 
FROM layoffs_staging2
WHERE row_num >1;

SELECT *
FROM layoffs_staging2;

-- 2. Standardizing The Data
-- Viewing any whitespace from company values.
SELECT company, TRIM(company)
FROM layoffs_staging2
GROUP BY company
ORDER BY company DESC;

-- Updating the companys column to remove whitespace after name.
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT company 
FROM layoffs_staging2 
GROUP BY company
ORDER BY company DESC;


-- Viewing all the industries in the data.
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;
-- Industry column contains Nulls and Blanks

-- Cleaning/aggregating Crypto industries into one industry
-- The crypto industry has some variations. Instead of redundent responses, aggregating same industry responses together.
SELECT *
FROM layoffs_staging2
WHERE industry LIKE '%Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';
-- Three rows where affected.


-- Viewing the country column and trimming the period from the United States.
SELECT DISTINCT country, TRIM(TRAILING '.'FROM country)
FROM layoffs_staging2
ORDER BY 1 DESC;

-- Updating the country column to fix duplicate values of United States.
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.'FROM country)
WHERE country LIKE 'United States%';
-- Four records were cleaned.

-- The duplicate United States entry has been deleted from the table.
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1 DESC;


-- Converting date column from string to date format for time series.
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

-- Updating the table's column to the new format of the date.
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

-- I have now converted the format of the text date but not the column data type.
SELECT `date`
FROM layoffs_staging2;

-- Modifying the data type of the date column.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. Fixing Nulls and Blanks
-- Viewing records which have null values for both numeric columns.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- The industry column has nulls and blank values.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- Updating the blanks to nulls so they are treated the same.
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Airbnb has multiple rows so I can use the one row with the value to populate the null in the other row.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Self joining my table to check if there is missing value in the first table t1 that is not missing in the second table t2, if so then update that null with the value from t2.
SELECT *, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	-- joining where the company AND location is a match
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;
-- There are three different companies have industry values that can replace the nulls.

-- Updating the staging table t1 with the industry values from t2 where t1 industry is null.
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; 

-- Both Airbnb rows are now updated with the correct industry for each row.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- After checking again for nulls, only one company has a null for industry because there is no other data for that company, meaning only had one group of layoffs in the data set.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


-- 4. Removing Useless Rows/Columns
-- Can you delete the data vs should you delete the data?
-- I am working with Layoffs data, if the company is missing the number of layoffs and percentage laid off, that company is useless in my data set for analysis.
-- Therefore, removing these useless rows, may save on resources and execution times when querying the data in the future.

-- Deleting the rows unnessesary for analysis.
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;
-- 361 rows were delete from the table.

-- No longer need the row number column I added previously.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final Cleaned Data
SELECT *
FROM layoffs_staging2;
-- From 2,361 rows to 1,995 rows