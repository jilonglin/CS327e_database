--Our SDGCountry, SDGSeries tables are already in good format, so we do not need to change it. For our SDGData table, the Country_Name and Indicator_Name columns vilate 2NF, and they should be in SDGCountry and SDGSeries table, respectively, so we need to delete them. And for SDGCountry_Series table, the DESCRIPTION column should be joined to the SDGData table. 
--As a result, we can replace the SDGData with SDGInfo.


CREATE TABLE dataset_1.SDGInfo AS
SELECT d.Country_Code, d.Indicator_Code, Y1990, Y1991, Y1992, Y1993, Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003,Y2004,Y2005,Y2006,Y2007,Y2008,Y2009,Y2010,Y2011,Y2012,Y2013,Y2014,Y2015,Y2016,Y2017,Y2018,s.DESCRIPTION
FROM `dataset_1.SDGData` d
LEFT JOIN `dataset_1.SDGCountry_Series` s
ON d.Indicator_Code=s.Series_Code and d.Country_Code=s.Country_Code
ORDER BY Country_Code
