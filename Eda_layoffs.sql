-- Exploratory Data Analysis

-- Retrieve all data from the layoffs staging table
SELECT * FROM layoffs_staging2;

-- Get the maximum number of layoffs and the highest percentage of layoffs in a company
SELECT 
    MAX(total_laid_off) AS max_layoffs, 
    MAX(percentage_laid_off) AS max_percentage_laid_off
FROM layoffs_staging2;

-- Retrieve companies where all employees were laid off, ordered by the highest funds raised
SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1 
ORDER BY funds_raised_millions DESC;

-- Get the total layoffs per company, ordered by highest layoffs
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2 
GROUP BY company
ORDER BY total_layoffs DESC;

-- Find the earliest and latest recorded layoffs date
SELECT MIN(`date`) AS earliest_date, MAX(`date`) AS latest_date
FROM layoffs_staging2;

-- Get total layoffs by industry, ordered by highest layoffs
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_layoffs DESC;

-- Get total layoffs by country, ordered by highest layoffs
SELECT country, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY country
ORDER BY total_layoffs DESC;

-- Get total layoffs per year, ordered by recent years first
SELECT YEAR(`date`) AS layoff_year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2 
GROUP BY layoff_year
ORDER BY layoff_year DESC;

-- Get total layoffs per month across all years, ordered by highest layoffs
SELECT MONTH(`date`) AS layoff_month, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2 
GROUP BY layoff_month
ORDER BY total_layoffs DESC;

-- Get total layoffs by company stage, ordered by highest layoffs
SELECT stage, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2 
GROUP BY stage
ORDER BY total_layoffs DESC;

-- Get total layoffs per month (formatted as YYYY-MM), ordered by highest layoffs
SELECT 
    SUBSTRING(`date`, 1, 7) AS `month`, 
    SUM(total_laid_off) AS total_sum
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY `month`
ORDER BY total_sum DESC;

-- Compute rolling total of layoffs per month using a CTE
WITH Rolling_Total AS (
    SELECT 
        SUBSTRING(`date`, 1, 7) AS `month`, 
        SUM(total_laid_off) AS total_sum
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `month`
    ORDER BY `month` ASC
)
SELECT 
    `month`, 
    total_sum, 
    SUM(total_sum) OVER(ORDER BY `month`) AS rolling_total
FROM Rolling_Total;

-- Get the total layoffs per company per year, ordered by highest layoffs
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY total_layoffs DESC;

-- Find the top 5 companies with the highest layoffs per year using CTEs
WITH Company_Year AS (
    SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_layoffs
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS (
    SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
    FROM Company_Year
    WHERE years IS NOT NULL
)
SELECT * FROM Company_Year_Rank WHERE Ranking <= 5;
