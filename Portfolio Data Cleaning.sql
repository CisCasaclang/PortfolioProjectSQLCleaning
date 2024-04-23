
##Data Cleaning


SELECT *
FROM parks_and_recreation.layoffs
;


## 1. Remove Duplicates
## 2. Standardize the Data
## 3. Null Values or Blank Values
## 4. Remove Any Columns


CREATE TABLE layoffs_staging
Like layoffs
;


INSERT layoffs_staging
SELECT *
FROM layoffs
;


SELECT *
FROM layoffs_staging
;


## 1. Remove Duplicates

SELECT *
FROM layoffs_staging
;


SELECT *,
ROW_NUMBER() OVER(Partition By company, industry, total_laid_off, percentage_laid_off, 'date') AS RowNum
FROM layoffs_staging
;


WITH duplicates_cte AS(
SELECT *,
ROW_NUMBER() OVER(Partition By company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS RowNum
FROM layoffs_staging
)
SELECT *
FROM duplicates_cte
WHERE RowNum > 1
;




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
  `RowNum` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2
;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(Partition By company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS RowNum
FROM layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE RowNum > 1
;




## 2. Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET company = TRIM(company)
;


SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
;


SELECT DISTINCT industry
FROM layoffs_staging2
;


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;


SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;


SELECT date
FROM layoffs_staging2
;

SELECT date,
STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y')
;


ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE
;



## 3. Null and Blank Values


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;


UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
;

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'
;


SELECT t1.industry, t2.industry
FROM layoffs_staging2  t1
JOIN layoffs_staging2  t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

UPDATE layoffs_staging2  t1
JOIN layoffs_staging2  t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL
;




## 4. Delete unnecessary columns


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;


SELECT *
FROM layoffs_xstaging2
;


ALTER TABLE layoffs_staging2
DROP COLUMN RowNum
;

