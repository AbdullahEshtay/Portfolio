-- data cleaning 

select *
from layoffs;

-- 1 remove duplicates
-- 2 standardize the data
-- 3 null values
-- 4remove any columns

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select * 
from layoffs;
 
with duplicates_cte As
(
select * ,
row_number() 
over(partition by company,location , industry , total_laid_off,percentage_laid_off,`date`,stage,country,
funds_raised_millions) as Row_num
from layoffs_staging
)
select * 
from duplicates_cte
where Row_num >1;

select*
from layoffs_staging
where company = 'casper';

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
  `Row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
delete 
from layoffs_staging2
where Row_num >1;

select *
from layoffs_staging2;


insert into layoffs_staging2
select * ,
row_number() 
over(partition by company,location , industry , total_laid_off,percentage_laid_off,`date`,stage,country,
funds_raised_millions) as Row_num
from layoffs_staging;



-- standardizing data
select company ,trim(company) 
from layoffs_staging2;

update layoffs_staging2
set company= trim(company);

select  distinct industry 
from  layoffs_staging2
order by 1;

select *
from layoffs_staging2;


update layoffs_staging2
set industry= 'Crypto'
where industry like 'Crypto%';

select distinct country,trim(Trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(Trailing '.' from country) 
where country like 'United States%';

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2	
set `date`= str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` Date ;
select *
from layoffs_staging2;

update layoffs_staging2
set industry = null
where industry = '';

select  st1.industry,st2.industry
from layoffs_staging2 st1
join layoffs_staging2 st2
	on st1.company=st2.company
where (st1.industry is null or st1.industry = '')
and st2.industry is not null;

update layoffs_staging2 as st1
join layoffs_staging2 as st2
	ON st1.company=st2.company
 set st1.industry=st2.industry
 where  st1.industry is null 
 and st2.industry is not null;
 
 select *
 from layoffs_staging2	
 where company like 'bALLY%';
 
 select *
 from layoffs_staging2	
 where total_laid_off is null
 and percentage_laid_off is null;
 
 delete
 from layoffs_staging2	
 where total_laid_off is null
 and percentage_laid_off is null;
 
-- remove the coloumn that we did
alter table layoffs_staging2	
drop column Row_num;

select *
from layoffs_staging2;

-- Exploratory Data Analysis
select max(total_laid_off),max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off=1
order by funds_raised_millions desc;

Select industry ,sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

Select comany ,sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select MIn(`date`),max(`date`)
from layoffs_staging2;

select substring(`date`,1,7) as `month` , sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

with Rolling_Total as
(
select substring(`date`,1,7) as `month` , sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`,total_off,sum(total_off)over(order by `month`) as rolling_total
from rolling_total;






select *
from layoffs_staging2;