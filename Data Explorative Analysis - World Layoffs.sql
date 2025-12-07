-- exploring the same set of data I just cleaned 

SELECT *
FROM layoffs_staging2; 

SELECT max(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2; 

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
order by funds_raised_millions desc; 

SELECT company, sum(total_laid_off)
FROM layoffs_staging2
group by company
order by 2 desc; 

SELECT min(`date`), max(`date`)
FROM layoffs_staging2; 

SELECT industry, sum(total_laid_off)
FROM layoffs_staging2
group by industry
order by 2 desc; 

SELECT country, sum(total_laid_off)
FROM layoffs_staging2
group by country
order by 2 desc;

SELECT year(`date`), sum(total_laid_off)
FROM layoffs_staging2
group by year(`date`)
order by 1 desc;

SELECT stage, sum(total_laid_off)
FROM layoffs_staging2
group by stage
order by 2  desc;


SELECT company, avg(percentage_laid_off)
FROM layoffs_staging2
group by company
order by 2 desc;


SELECT substring(`date`, 1, 7) as `month`, sum(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc; 

with rolling_total as 
(
SELECT substring(`date`, 1, 7) as `month`, sum(total_laid_off) as total_off
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) is not null
group by `month`
order by 1 asc
)
SELECT `month`, total_off, sum(total_off) over (order by `month`) as roll_tot
FROM rolling_total; 


SELECT company, sum(total_laid_off) 
FROM layoffs_staging2
group by company
order by 2 desc; 


SELECT company, year(`date`), sum(total_laid_off) 
FROM layoffs_staging2
group by company, year(`date`)
order by company asc; 

with Company_Year (company, years, total_laid_off) as
(
SELECT company, year(`date`), sum(total_laid_off) 
FROM layoffs_staging2
group by company, year(`date`)
), 
Company_Year_Rank as 
(
SELECT *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
FROM Company_Year
WHERE years is not null
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5; 















