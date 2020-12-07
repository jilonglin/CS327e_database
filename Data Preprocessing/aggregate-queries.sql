-- To find the number of countries in each income group.
select Income_Group, Count(*) as num_income_group
from `dataset_1.SDGCountry`
Group By Income_Group

-- To find the number of different regions, and how many countries are in each region.
select Region, count(*) as num_of_regions
from `dataset_1.SDGCountry`
Group By Region

-- Calculate the total forest area covered in the world in 2016. 
SELECT SUM(Y2016) AS TOTAL_Forest_Area
FROM dataset_1.SDGInfo
WHERE Indicator_Code="AG.LND.FRST.K2" 

-- Calculate the average value of each indicator
select a.Indicator_Code, s.Indicator_Name, AVG(Average_value) AS Indicator_Average
from dataset_1.SDGdata_average a join dataset_1.SDGSeries s on a.Indicator_Code = s.Series_Code
group by a.Indicator_Code, s.Indicator_Name

-- Count how many countries that 100% of its population has access to electricity in 2016. 
-- The resule shows 121 out of 263 countries that 100% of its population has access to electricity in 2016.
SELECT COUNT(Indicator_Code) as MAX_Y2016
FROM dataset_1.SDGInfo
WHERE Indicator_Code="EG.ELC.ACCS.ZS" and Y2016=100

-- To find countries where over 90% of people have access to clean fuels and technologies for cooking every year
select Country_Code, min(Value) as minimum_value
from `dataset_1.SDGdata_all`
where Indicator_Code = 'EG.CFT.ACCS.ZS'
Group By Country_Code
Having min(Value) >= 90
Order By min(Value) desc

-- To find countries which withdraw more water annually than its internal resources
select Country_Code, AVG(Value) as average_value
from `dataset_1.SDGdata_all`
where Indicator_Code = 'ER.H2O.FWTL.ZS'
Group By Country_Code
Having AVG(Value) >= 100
Order By AVG(Value) desc

--To find countries that Renewable energy consumption is greater than 80% of total final energy consumption every year
select Country_Code, min(Value) as minimum_value
from `dataset_1.SDGdata_all`
where Indicator_Code = 'EG.FEC.RNEW.ZS'
Group By Country_Code
Having min(Value) >= 80
Order By min(Value) desc


