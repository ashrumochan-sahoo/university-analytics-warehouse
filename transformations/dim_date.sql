-- ============================================================
-- Populate dim_date dimension
-- ============================================================

USE university_dw;

-- Clear existing data
DELETE FROM dim_date;

-- Generate dates from 2018-01-01 to 2030-12-31
DECLARE @StartDate DATE = '2018-01-01';
DECLARE @EndDate DATE = '2030-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO dim_date (
        date_key, full_date, year, quarter, month, month_name,
        day, day_of_week, day_name, week_of_year, is_weekend
    )
    VALUES (
        CAST(FORMAT(@StartDate, 'yyyyMMdd') AS INT),
        @StartDate,
        YEAR(@StartDate),
        DATEPART(QUARTER, @StartDate),
        MONTH(@StartDate),
        DATENAME(MONTH, @StartDate),
        DAY(@StartDate),
        DATEPART(WEEKDAY, @StartDate),
        DATENAME(WEEKDAY, @StartDate),
        DATEPART(WEEK, @StartDate),
        CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END
    );
    
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;

-- Verify
SELECT COUNT(*) AS total_dates FROM dim_date;
SELECT TOP 5 * FROM dim_date ORDER BY date_key;