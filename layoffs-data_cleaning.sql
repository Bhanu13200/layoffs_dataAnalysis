-- Data cleaning

SELECT 
    *
FROM
    layoffs;
    
    ---------------------------------------

CREATE TABLE layoffs_staging LIKE layoffs; -- creates a copy(duplicate) table of layoffs for not disturb the raw data 

SELECT 
    *
FROM
    layoffs_staging; -- get the data type from layoffs after create table

insert layoffs_staging select * from layoffs; -- inserted all the data from layoffs
-------------------------------------------------------------------------------------------------------------------
-- 1. remove duplicates
   select * from layoffs_staging;
   
   -- find duplicates  using sub-query and window funtion
  
  select *
  from (select *,
  row_number() over ( partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)as row_num 
  from layoffs_staging ) as layoffs
  where row_num > 1;
  
---------------------------------------------------------------------------------------------------------------------
-- find duplicates  using cte and window funtion
  with duplicate_cte as
  (select *,
  row_number() over ( partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)as row_num 
  from layoffs_staging
  )
  select * 
  from duplicate_cte
  where row_num >1;
  ---------------------------------------------------------------------------------
  
  -- using group by
  select  *,count(*)
  from layoffs_staging group by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions  having count(*)>1;
     
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2  select *,
  row_number() over ( partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions)as row_num 
  from layoffs_staging;
  
  
  -- delect duplicate from table
  SET SQL_SAFE_UPDATES = 0;

  
  delete from layoffs_staging2
  where row_num >1;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 2. standardize the data

select company,trim(company) from layoffs_staging2;

update  layoffs_staging2
set company = trim(company);

select
distinct industry from layoffs_staging2
order by 1;

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';

select
distinct country,trim(trailing '.'from country) from layoffs_staging2
order by 1;

update layoffs_staging2
set  country = trim(trailing '.'from country)
where country like 'United States%';

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

 -- update text to date format
 
update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

-- changed data type
alter table  layoffs_staging2 modify column `date` date;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3. null values or black values

select * 
from layoffs_staging2 
where industry is null or industry = '';

update layoffs_staging2
 set industry = null
 where industry = '';

select * 
from layoffs_staging2 
where industry is null ;

select t1.industry,t2.industry 
from layoffs_staging2 t1 
join layoffs_staging2 t2
	 on t1.company = t2.company
	 and t1.location = t2.location
 where (t1.industry is null or t1.industry = '') and t2.industry is not null;
  
 update layoffs_staging2 t1 
join layoffs_staging2 t2
     on t1.company = t2.company
 set t1.industry =t2.industry  
 where t1.industry is null  and t2.industry is not null;
 
 
select * 
from layoffs_staging2 
where company like 'Bally%';

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
        
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 4. remove any columns

select * from layoffs_staging2;
-- we don't need row_num any more 
alter table layoffs_staging2 drop column row_num;



