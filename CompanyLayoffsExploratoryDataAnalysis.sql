-- Exploratory Data Analysis

SELECT * 
FROM layoffs_staging2;


-- Viewing distinct companies in data set.
SELECT DISTINCT company, industry
FROM layoffs_staging2
ORDER BY company;
-- 1640 different companies in data set.

-- Viewing distinct industries in data set.
SELECT DISTINCT industry
FROM layoffs_staging2;
-- 30 different industries in data set.

-- Viewing number of companies per industry.
SELECT
	industry,
    COUNT(DISTINCT company) AS num_of_companies
FROM layoffs_staging2
GROUP BY industry
ORDER BY num_of_companies DESC;


-- Viewing count of companies and layoffs per industry.
SELECT *,
	ROUND(num_of_layoffs/num_of_companies, 0) AS layoffs_per_company_ratio
FROM (
SELECT 
	industry, 
	COUNT(DISTINCT company) AS num_of_companies, 
	SUM(total_laid_off) AS num_of_layoffs
FROM layoffs_staging2
GROUP BY industry
ORDER BY num_of_layoffs DESC) subquery;

-- Viewing total layoffs and companies per country.
SELECT *,
	ROUND(num_of_layoffs/num_of_companies, 0) AS layoffs_per_company_ratio
FROM (
SELECT 
	country, 
	COUNT(DISTINCT company) AS num_of_companies, 
	SUM(total_laid_off) AS num_of_layoffs
FROM layoffs_staging2
GROUP BY country
HAVING num_of_layoffs IS NOT NULL
ORDER BY num_of_layoffs DESC) subquery;


-- CLOSED DOWN COMPANIES (1OO% laid off)
-- Viewing all companys that laid off 100% of employees (fully went under).
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;
-- 116 companies laid off 100% of employees.


-- Calculating the percentage of closed down companies out of total companies.
-- (# of 100% laid off / # of total companies in table) * 100
WITH 
total_companies AS
(
SELECT COUNT(*) AS company_count 
FROM
	(SELECT DISTINCT company, industry
	FROM layoffs_staging2) sq1
),
closed_companies AS
(
SELECT COUNT(company) AS closed_comp_count
FROM layoffs_staging2
WHERE percentage_laid_off = 1
)
SELECT *,
	ROUND((closed_comp_count/company_count) * 100,2) AS percent_total
FROM total_companies
JOIN closed_companies;
-- 7% of company layoffs were fully closed down.


-- Calculating percent totals of closed down companies by country.
WITH total_companies AS
(
SELECT country, COUNT(DISTINCT company) AS company_count
FROM layoffs_staging2
GROUP BY country
),
closed_companies AS
(
SELECT country, COUNT(company) AS closed_comp_count
FROM layoffs_staging2
WHERE percentage_laid_off = 1
GROUP BY country
)
SELECT 
	total.country, company_count, closed_comp_count,
	ROUND((closed_comp_count/company_count) * 100,2) AS percent_total
FROM total_companies total
JOIN closed_companies closed
	ON total.country = closed.country
ORDER BY percent_total DESC;


-- Calculating percent totals of closed down companies by industry.
WITH total_companies AS
(
SELECT industry, COUNT(DISTINCT company) AS company_count
FROM layoffs_staging2
GROUP BY industry
),
closed_companies AS
(
SELECT industry, COUNT(company) AS closed_comp_count
FROM layoffs_staging2
WHERE percentage_laid_off = 1
GROUP BY industry
)
SELECT total.industry, company_count, closed_comp_count,
	ROUND((closed_comp_count/company_count) * 100,2) AS percent_total
FROM total_companies total
JOIN closed_companies closed
	ON total.industry = closed.industry
ORDER BY percent_total DESC;


-- Viewing all closed companies by their raised capital.
SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- Percent totals of closed companies per business stage.
WITH total_companies AS
(
SELECT stage, COUNT(DISTINCT company) AS company_count
FROM layoffs_staging2
GROUP BY stage
),
closed_companies AS
(
SELECT stage, COUNT(company) AS closed_comp_count
FROM layoffs_staging2
WHERE percentage_laid_off = 1
GROUP BY stage
),
stage_layoffs AS
(
SELECT stage, SUM(total_laid_off) as total_layoffs
FROM layoffs_staging2
GROUP BY stage
)
SELECT total.stage, company_count, closed_comp_count,
	ROUND((closed_comp_count/company_count) * 100,2) AS percent_total,
	DENSE_RANK() OVER(ORDER BY closed_comp_count DESC) AS closures_rank,
    total_layoffs,
    DENSE_RANK() OVER(ORDER BY total_layoffs DESC) AS layoffs_rank
FROM total_companies total
JOIN closed_companies closed
	ON total.stage = closed.stage
JOIN stage_layoffs layoffs
	ON closed.stage = layoffs.stage
ORDER BY percent_total DESC;


-- TOTAL LAYOFFS
-- Companies total layoffs.
SELECT 
	company, industry,
	SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company, industry
ORDER BY 3 DESC
LIMIT 20;
-- Amazon, Google, & Meta laid off the most employees.


-- LAYOFFS BY TIME/DATE
-- Viewing the min and max dates in our data set.
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;
-- The layoffs range from 3/11/2020 to 3/6/2023.


-- Layoffs by year
SELECT 
	YEAR(`date`) AS `Year`, 
	SUM(total_laid_off) AS total_layoffs,
    COUNT(DISTINCT company, location) AS num_of_companies
FROM layoffs_staging2
GROUP BY YEAR(`date`)
HAVING `Year` IS NOT NULL
ORDER BY `Year` ASC;


-- Rolling sum of layoffs using a CTE
WITH rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY `Month`
)
SELECT 
	`Month`, 
    total_layoffs,
	SUM(total_layoffs) OVER (ORDER BY `Month`) AS rolling_total
FROM rolling_total;

-- Viewing the top 15 largest layoffs
SELECT company, industry,
	YEAR(`date`) AS year, 
	SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company, industry, YEAR(`date`)
ORDER BY total_layoffs DESC
LIMIT 15;

-- Ranking companies by number of layoffs for each year
-- CTE to calculate the total layoffs for each year
WITH company_year (company, industry, `year`, total_laid_off) AS
(
SELECT company, industry, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, industry, YEAR(`date`)
ORDER BY 4 DESC
),
-- CTE to rank the companies layoffs 
company_rank AS
(
SELECT *,
	DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_laid_off DESC) AS layoff_ranking
FROM company_year
WHERE `year` IS NOT NULL
)
-- filtering off the rank
SELECT *
FROM company_rank
WHERE layoff_ranking <= 5;
-- Querying the top 5 companies with the most layoffs for each year.

