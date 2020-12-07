-- To see indicator values of different income groups in 2016
select i.Country_Code, d.Short_Name, i.Indicator_Code, d.Income_Group, i.Y2016
from `dataset_1.SDGInfo` i left join `dataset_1.SDGCountry` d on i.Country_Code = d.Country_Code
order by d.Income_Group desc


-- To identify countries and indicators that do not have description/source
select i.Indicator_Code, i.Country_Code, s.DESCRIPTION
from `dataset_1.SDGInfo` i left join `dataset_1.SDGCountry_Series` s on i.Indicator_Code = s.Series_Code and i.Country_Code = s.Country_Code
order by s.DESCRIPTION


-- To find the percentage of population with access to clean fuels and technologies for cooking in high income countries
select i.Country_Code, i.Indicator_Code, i.Y2000, i.Y2001, i.Y2002, i.Y2003, i.Y2004, i.Y2005, i.Y2006, i.Y2007, i.Y2008, d.Income_Group
from `dataset_1.SDGInfo` i right join `dataset_1.SDGCountry` d on i.Country_Code = d.Country_Code
where d.Income_Group = 'High income' and i.Indicator_Code= "EG.CFT.ACCS.ZS"
order by i.Y2008 DESC 


-- For each indicator, find the region(s) that has(have) highest indicator values in Y2008
select d.Region, i.Country_Code, i.Indicator_Code, i.Y2008
from `dataset_1.SDGInfo` i right join `dataset_1.SDGCountry` d on i.Country_Code = d.Country_Code
where d.Region != '' and i.Y2008 is not null
order by i.Indicator_Code, i.Y2008 desc



-- To descendingly order environment-related emissions by different countries in Y2008
select a.Topic, i.Country_Code, i.Indicator_Code, i.Y2008
from `dataset_1.SDGSeries` a left join `dataset_1.SDGInfo` i on a.Series_Code = i.Indicator_Code
where a.Topic =  'Environment: Emissions'
order by i.Indicator_Code, i.Y2008 desc



-- To compare and contrast Chinese and the US performance on sustainable development
select c.Government_Accounting_concept, c.Latest_trade_data, c.Region, c.Income_Group, i.*
from `dataset_1.SDGCountry` c right join `dataset_1.SDGInfo` i on c.Country_Code = i.Country_Code
where c.Country_Code = 'CHN' or c.Country_Code = 'USA'
order by i.Indicator_Code

