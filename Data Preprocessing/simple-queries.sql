--For the SDGData Table, we find the countries in 2016 that have high percentage of population that have access to clean fuels and technologies for cooking
SELECT Country_Name, Indicator_Name, Y2016
FROM [composite-map-230320:dataset_1.SDGData]
WHERE Indicator_Name = "Access to clean fuels and technologies for cooking (% of population)"
ORDER BY Y2016 DESC


--For the SDGCountry Table, we find the high income countries in Europe and Central Asia
SELECT _Country_Code_, Short_Name, Special_Notes
FROM [composite-map-230320:dataset_1.SDGCountry]
WHERE Income_Group = "High income" AND Region = "Europe & Central Asia"
ORDER BY Short_Name

--For the SDGSeries table, we want to find the Series_Code of the indicator on the topic "Environment: Energy production & use"
SELECT Series_Code, Topic, Indicator_Name, Long_definition, Statistical_concept_and_methodology
FROM [composite-map-230320:dataset_1.SDGSeries]
WHERE Topic = "Environment: Energy production & use"
ORDER BY Series_Code

--For the SDGCountry_Series table, we want to know the sources from which data of China was collected 
SELECT CountryCode, SeriesCode, DESCRIPTION
FROM [composite-map-230320:dataset_1.SDGCountry_Series]
WHERE CountryCode = "CHN"
ORDER BY SeriesCode

