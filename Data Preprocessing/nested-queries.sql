--find the countries and their indicator where their indicator values are above average in 2014
select i.Country_Code, i.Indicator_Code, Y2014, s.Topic, s.Indicator_Name
from dataset_1.SDGInfo i
left join
(select Indicator_Code, AVG(Y2014) as average2014
from dataset_1.SDGInfo
group by Indicator_Code) as a
on i.Indicator_Code=a.Indicator_Code
left join dataset_1.SDGSeries s on i.Indicator_Code=s.Series_Code
where Y2014>a.average2014

--for each indicator find the highest value and which countries have this highest value
select Country_Code, i.Indicator_Code, Y2014
from dataset_1.SDGInfo i
join
(select Indicator_Code, MAX(Y2014) as MAX
from dataset_1.SDGInfo
group by Indicator_Code) as a
on i.Indicator_Code=a.Indicator_Code and i.Y2014=a.MAX
order by a.Indicator_Code


--find which parts of education input that China are below average
select c.Country_Code, c.Indicator_Code, s.Indicator_Name, c.Average_value as CHN_avg, b.avg as WORLD_avg
from dataset_1.SDGdata_average c
right join
(select Indicator_Code,  AVG(Average_value) as avg
from dataset_1.SDGdata_average
group by Indicator_Code
having indicator_Code in
(select Series_Code
from dataset_1.SDGSeries
where Topic="Education: Inputs")) as b
on c.Indicator_Code=b.Indicator_Code
join dataset_1.SDGSeries as s
on s.Series_Code=c.Indicator_Code
where c.Country_Code = "CHN" and c.Average_value<b.avg


--find what kinds of mortality rates that China are higher than world's average
select c.Country_Code, c.Indicator_Code, s.Indicator_Name, c.Average_value as CHN_mortality, b.avg as GLOBAL_mortality
from dataset_1.SDGdata_average c
right join
(select Indicator_Code,  AVG(Average_value) as avg
from dataset_1.SDGdata_average
group by Indicator_Code
having indicator_Code in
(select Series_Code
from dataset_1.SDGSeries
where Topic="Health: Mortality")) as b
on c.Indicator_Code=b.Indicator_Code
join dataset_1.SDGSeries as s
on s.Series_Code=c.Indicator_Code
where c.Country_Code = "CHN" and c.Average_value>b.avg

--find all 2014 environmental emission values of China and compare the values with the world average
select Country_Code, a.Indicator_Code, s.Indicator_Name, Y2014 as CHN_Y2014, global_average as global_Y2014
from dataset_1.SDGInfo a
join
(select Indicator_Code, AVG(Y2014) as global_average
from dataset_1.SDGInfo i
group by Indicator_Code
having Indicator_Code in
(select Series_Code
from dataset_1.SDGSeries
where Topic = 'Environment: Emissions')) as b
on a.Indicator_Code = b.Indicator_Code
join dataset_1.SDGSeries s on a.Indicator_Code = s.Series_Code
where Country_Code = 'CHN'

--find people of what Income_group expose to the most PM2.5 air pollution in 2014
select b.Indicator_Code, Indicator_Name, Income_group, COUNT (Income_group) as count, AVG(Y2014 - global_average) as higher_than_average
from dataset_1.SDGInfo a
join
(select Indicator_Code, AVG(Y2014) as global_average
from dataset_1.SDGInfo i
group by Indicator_Code
having Indicator_Code = 'EN.ATM.PM25.MC.M3') as b
on a.Indicator_Code = b.Indicator_Code
join dataset_1.SDGSeries s on a.Indicator_Code = s.Series_Code
join dataset_1.SDGCountry c on c.Country_Code = a.Country_Code
where Y2014 > global_average and Income_group is not null
group by Income_group, b.Indicator_Code, Indicator_Name

-- find the regions where Annual freshwater withdrawals, total (% of internal resources) is larger than global average in 2014
select b.Indicator_Code, Indicator_Name, Region, COUNT (Region) as count, AVG(Y2014) as Y2014, AVG(global_average) as global_average
from dataset_1.SDGInfo a
join
(select Indicator_Code, AVG(Y2014) as global_average
from dataset_1.SDGInfo i
group by Indicator_Code
having Indicator_Code = 'ER.H2O.FWTL.ZS') as b
on a.Indicator_Code = b.Indicator_Code
join dataset_1.SDGSeries s on a.Indicator_Code = s.Series_Code
join dataset_1.SDGCountry c on c.Country_Code = a.Country_Code
where Y2014 > global_average and region is not null
group by Region, b.Indicator_Code, Indicator_Name

-- find GDP and GDP per capita trend since 1990, comparing the value each year with the global average value
select Country_Code, a.Indicator_Code, b.Indicator_Name, a.Year, (Value - c.average_GDP) as difference_with_global_average
from dataset_1.SDGdata_all a
join dataset_1.SDGSeries b
on a.Indicator_Code = b.Series_Code
join
(select Indicator_Code, Year, AVG(Value) as average_GDP
from dataset_1.SDGdata_all
group by Year, Indicator_Code
having (Indicator_Code = 'NY.GDP.MKTP.CD' or Indicator_Code = 'NY.GDP.PCAP.CD') and Year != 2018
order by Year) as c
on a.Year = c.Year and a.Indicator_Code = c.Indicator_Code
where Country_Code = 'CHN' and (a.Indicator_Code = 'NY.GDP.MKTP.CD' or a.Indicator_Code = 'NY.GDP.PCAP.CD')
