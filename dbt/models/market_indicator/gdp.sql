with src as (
	select 
		country_value as country, 
		date as year, 
		value as gdp, 
		COALESCE(lag(value, 1) OVER(partition by country_value order by date), 0) as previous_year_gdp
	from market_indicator
	where value is not null 
)
, gdp_growth as (
    select * , 
		CASE WHEN previous_year_gdp = 0 THEN 0 
			 ELSE (gdp-previous_year_gdp)/previous_year_gdp END as gdp_growth 
	from src
    )
, min_max_gdp as (
    select *, 
		row_number() over(partition by country order by gdp_growth) as min_gdp, 
		row_number() over(partition by country order by gdp_growth desc) as max_gdp 
	from gdp_growth
    where year >= '2000' and year < '2022'
)
, min_gdp as (
	select * from min_max_gdp
	where min_gdp = 1
)
, max_gdp as (
	select * from min_max_gdp
	where max_gdp = 1
)
select a.country,
		a.year,
		a.gdp,
		a.gdp_growth,
		b.gdp_growth as min_gdp_growth_since_2000, 
		c.gdp_growth as max_gdp_growth_since_2000 
from min_max_gdp a
left join min_gdp b on a.country = b.country
left join max_gdp c on a.country = c.country
order by country, year
