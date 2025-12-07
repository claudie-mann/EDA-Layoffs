SELECT *
FROM layoffs; 


-- 1. remove duplicates
-- 2. standardise the data
-- 3. null values / blank values
-- 4. remove columns or rows (or should you?) 


create table layoffs_staging
like layoffs; 

SELECT *
FROM layoffs_staging; 

insert layoffs_staging
SELECT * 
FROM layoffs; 

-- 1. remove duplicates
SELECT *, 
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging; 

with duplicate_cte as 
(
	SELECT *, 
	row_number() over(
	partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1; -- note the partition by is different FROM before getting turned into a CTE

-- below double checking manually and realised he needed to partition by every single column in order to filter out the duplicates correctly
SELECT *
FROM layoffs_staging
WHERE company = 'oda';

SELECT *
FROM layoffs_staging
WHERE company = 'casper';

-- creating a new table to perform duplicate deletions
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

SELECT *
FROM layoffs_staging2; 

insert into layoffs_staging2
SELECT *, 
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging; 

DELETE
FROM layoffs_staging2
WHERE row_num > 1; 


-- 2. standardising data
SELECT company, trim(company)
FROM layoffs_staging2;

update layoffs_staging2
set company = trim(company);

SELECT distinct industry
FROM layoffs_staging2
order by 1; -- then identified we could organise industry name about 'crypto'

SELECT *
FROM layoffs_staging2
WHERE industry like 'Crypto%'; 

update layoffs_staging2
set industry = 'Crypto'
WHERE industry like 'crypto%';

SELECT distinct country
FROM layoffs_staging2
order by 1;

SELECT *
FROM layoffs_staging2
WHERE country like 'united states%'
order by 1;

SELECT distinct country, trim(trailing '.' FROM country)
FROM layoffs_staging2
order by 1; 

update layoffs_staging2
set country = trim(trailing '.' FROM country)
WHERE country like 'United States%'; 


SELECT `date`, 
str_to_date(`date`, '%m/%d/%Y') -- case sensitive in this case for whatever reason... 
FROM layoffs_staging2; 

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2; 

-- changing it to a date column now after formatting it to the proper date format (was preiviously a 'text' column)
alter table layoffs_staging2
modify column `date` date; -- now you can see the data type of this column changed to 'date' rather than the original 'text'

-- 3. Nulls and blanks
SELECT *
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null; 
-- typing '= null' wouldn't have worked 

SELECT distinct industry
FROM layoffs_staging2; 

update layoffs_staging2
set industry = null
WHERE industry = ''; 

SELECT *
FROM layoffs_staging2
WHERE industry is null
or industry = ''; 

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb'; -- to populate the blank values

SELECT * -- using self join to populate
FROM layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
	and t1.location = t2.location
WHERE (t1.industry is null or t1.industry = '')
and t2.industry is not null; 

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry 
WHERE (t1.industry is null or t1.industry = '')
and t2.industry is not null; -- btw this didn't work lol so we're going back to row 134 or so and setting all the blanks into nulls first
-- and we ran it again and it worked 


SELECT * 
FROM layoffs_staging2
WHERE company like 'Bally%'; -- double checking to see if indeed there was no null or blank under 'industry'
-- cos this company doesn't have another row to populate/join

-- this way of populating data wouldn't apply to columns such as 'total laid off' or 'percentage laid off'
SELECT *
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null; 

delete 
FROM layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null; 

SELECT *
FROM layoffs_staging2;

alter table layoffs_staging2
drop column row_num; 


-- end of data cleaning project




