-- Find out the portion of money that countries characterized in high income group spend on each field?
select avg(Value) as avg, Indicator_Name
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
where i.Detail="%" and g.Country_Code in
(select Country_Code
from `composite-map-230320.dataset_1.SDGCountry`
where Income_Group="High income")
order by Value DESC)
group by Indicator_Name
order by avg DESC

--Find out countries in what income group spend the highest percent of their GERD
--(government expenditure in research and development) in each GERD field
--for instance, countries in high income group spend 35% of their GERD in engineering and technology, which is the highest among all income groups
-- on the other hand, countries in low income group spend 20% of their GERD in agriculture, which is the highest among all income groups
select y.Income_group, y.Indicator_Name as GERD_field, y.avg as Highest_percent
from
(select avg(Value) as avg, Indicator_Name, Income_group
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name, c.Income_Group
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
join `composite-map-230320.dataset_1.SDGCountry`  c on g.Country_Code = c.Country_Code
where i.Detail="%" and g.Indicator_code != 'EXPP_FS_FONS' and c.Income_Group="High income"
order by Value DESC)
group by Indicator_Name, Income_group

union all

select avg(Value) as avg, Indicator_Name, Income_group
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name, c.Income_Group
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
join `composite-map-230320.dataset_1.SDGCountry`  c on g.Country_Code = c.Country_Code
where i.Detail="%" and g.Indicator_code != 'EXPP_FS_FONS' and c.Income_Group="Low income"
order by Value DESC)
group by Indicator_Name, Income_group

union all
select avg(Value) as avg, Indicator_Name, Income_group
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name, c.Income_Group
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
join `composite-map-230320.dataset_1.SDGCountry`  c on g.Country_Code = c.Country_Code
where i.Detail="%" and g.Indicator_code != 'EXPP_FS_FONS' and c.Income_Group="Upper middle income"
order by Value DESC)
group by Indicator_Name, Income_group

union all

select avg(Value) as avg, Indicator_Name, Income_group
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name, c.Income_Group
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
join `composite-map-230320.dataset_1.SDGCountry`  c on g.Country_Code = c.Country_Code
where i.Detail="%" and g.Indicator_code != 'EXPP_FS_FONS' and c.Income_Group="Lower middle income"
order by Value DESC)
group by Indicator_Name, Income_group) as y

join

(select z.Indicator_Name, max(avg) as percent
from
(select avg(Value) as avg, Indicator_Name, Income_group
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name, c.Income_Group
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
join `composite-map-230320.dataset_1.SDGCountry`  c on g.Country_Code = c.Country_Code
where i.Detail="%" and g.Indicator_code != 'EXPP_FS_FONS' and c.Income_Group="High income"
order by Value DESC)
group by Indicator_Name, Income_group

union all

select avg(Value) as avg, Indicator_Name, Income_group
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name, c.Income_Group
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
join `composite-map-230320.dataset_1.SDGCountry`  c on g.Country_Code = c.Country_Code
where i.Detail="%" and g.Indicator_code != 'EXPP_FS_FONS' and c.Income_Group="Low income"
order by Value DESC)
group by Indicator_Name, Income_group

union all
select avg(Value) as avg, Indicator_Name, Income_group
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name, c.Income_Group
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
join `composite-map-230320.dataset_1.SDGCountry`  c on g.Country_Code = c.Country_Code
where i.Detail="%" and g.Indicator_code != 'EXPP_FS_FONS' and c.Income_Group="Upper middle income"
order by Value DESC)
group by Indicator_Name, Income_group

union all

select avg(Value) as avg, Indicator_Name, Income_group
from
(select g.Country_Code, g.Indicator_Code, g.Year, g.Value,i.Indicator_Name, c.Income_Group
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
join `composite-map-230320.dataset_1.SDGCountry`  c on g.Country_Code = c.Country_Code
where i.Detail="%" and g.Indicator_code != 'EXPP_FS_FONS' and c.Income_Group="Lower middle income"
order by Value DESC)
group by Indicator_Name, Income_group) as z
group by z.Indicator_Name) as x

on y.avg = x.percent

--Find out which countries increase their total government budget from 2010 to 2016
--but their research and development expenditure (% of GDP) decreases from 2010 to 2016
select a.Country_Code, s.Indicator_Name, c.table_Name, a.value - b.value as spending_decrease, e.budget2016 - e.budget2010 as total_budget_increase
from `composite-map-230320.dataset_1.SDGdata_all`  a
join `composite-map-230320.dataset_1.SDGCountry`  c on a.Country_Code = c.Country_Code
join `composite-map-230320.dataset_1.SDGSeries`  s on a.Indicator_Code = s.Series_Code
join `composite-map-230320.dataset_1.SDGdata_all`  b on a.Country_Code = b.Country_Code and a.Indicator_Code = 'GB.XPD.RSDV.GD.ZS' and a.Indicator_Code = b.Indicator_Code
join
(select a.Country_Code, a.Value_million_dollars as budget2010, b.Value_million_dollars as budget2016
from `composite-map-230320.dataset_2.Gov_budget`  a
join `composite-map-230320.dataset_2.Gov_budget`  b on a.Country_Code = b.Country_Code
where a.Year = 2010 and b.Year = 2016 and a.Value_million_dollars < b.Value_million_dollars and a.SEO = '_T') as e
on e.Country_Code = a.Country_Code
where a.Indicator_Code = 'GB.XPD.RSDV.GD.ZS' and a.year = 2010 and b.year = 2016 and a.value > b.value

--Find out what kinds of countries spend most of their money on Engineering and technology
--Since many countries such as US and China do not have GERD separated into different fields, the results are not conclusive
select c.Short_Name, a.Value
from `composite-map-230320.dataset_1.SDGCountry`  c
join
(select g.Country_Code, avg(Value) as Value
from `composite-map-230320.dataset_2.GERD`  g
left join `composite-map-230320.dataset_2.GERD_Indicators`  i
on g.Indicator_Code=i.Indicator_Code
where i.Indicator_Name="GERD - Engineering and technology" and i.Detail="In '000 current PPP$"
group by Country_Code) as a
on c.Country_Code=a.Country_Code
order by a.Value DESC
limit 10

-- Among countries that increased their government budget on Health from 2010 to 2016,
-- find out top ten of those that reduce the mortality rate by the largest amount
select c.Country_Code, co.short_name, c.Indicator_Code, c.value - d.value as mortality_rate_decrease
from `composite-map-230320.dataset_1.SDGdata_all` c
join `composite-map-230320.dataset_1.SDGCountry` co on c.Country_Code = co.Country_Code
join `composite-map-230320.dataset_1.SDGdata_all` d
on c.Country_Code = d.Country_Code and c.Indicator_Code = 'SH.DYN.NCOM.ZS' and c.Indicator_Code = d.Indicator_Code
join
(select a.Country_Code, a.SEO, a.Value_million_dollars as value1, b.Value_million_dollars as value2
from `composite-map-230320.dataset_2.Gov_budget` a
join `composite-map-230320.dataset_2.Gov_budget` b
on a.Country_Code = b.Country_Code and a.SEO = 'NABS07' and a.SEO = b.SEO
where a.year = 2010 and b.year = 2016 and a.Value_million_dollars < b.Value_million_dollars) as e
on c.Country_Code = e.Country_Code
where c.year = 2010 and d.year = 2016 and c.value > d.value
order by mortality_rate_decrease desc
limit 10

--Find out if countries with high pollution have little spending on Environment and Health
-- For all those countries that have data in GERD medical and health, find out those that have most PM2.5 emission
-- and compare their spending (percent GERD) in medical and health
select b.Short_Name, s.Indicator_Name, a.year, a.value as pollution, u.value as percent_GERD_in_medical_and_health
from `composite-map-230320.dataset_1.SDGdata_all` a
join `composite-map-230320.dataset_1.SDGCountry` b on a.Country_Code = b.Country_Code
join `composite-map-230320.dataset_1.SDGSeries`s on a.Indicator_Code = s.Series_Code
join
(select Country_Code, Indicator_Code, year, value
from `composite-map-230320.dataset_2.GERD`
where year = 2016 and Indicator_Code = 'EXPP_FS_MEDSCI') as u
on u.Country_Code = a.Country_Code
where a.Indicator_Code = 'EN.ATM.PM25.MC.M3' and a.year = 2016 and a.value is not null
order by a.value desc
limit 5
