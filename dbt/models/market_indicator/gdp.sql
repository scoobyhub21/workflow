with cte as (
	select country_value as country, date as year, value as gdp, lag(value, 1) OVER(partition by country_value order by date) as previous_year_gdp
	from {{ source('API_DATA', 'market_indicator') }}
	--where country_value = 'Australia' and value is not null 
	--where value is not null 
	--and date >= '1999' order by date
)
, gdp_growth as (
    select * , (gdp-previous_year_gdp)/previous_year_gdp as gdp_growth from cte
    )
, min_max_gdp as (
    select *, row_number() over(partition by country order by gdp_growth) as min_gdp, 
		   row_number() over(partition by country order by gdp_growth desc) as max_gdp from gdp_growth
    where year >= '2000'
)
select * from min_max_gdp 
--where country = 'Vietnam'
order by country, year;
