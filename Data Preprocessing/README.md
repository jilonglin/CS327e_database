Apache beam transformation:\
Since our tables are modified well, we basically did mathematical transformation on the table SDGInfo.

We calculated the average of the indicator value from 1990-2018 for each row.
Thus the return is [(Country_Code, Indicator_Code, year), average]

Also, we did row-to-column transformation. Our original columes include Country_Code, Indicator_Code, Y1990.........,Y2018.
We change our columns to Country_Code, Indicator_Code, Year, Indicator_Value. In other words, one row in our original table was spilited into 29 rows because there are 29 years.

Workflow:\
We keep getting this error:
set([(u'workflow', u'SEO_beam', datetime.datetime(2019, 5, 2, 0, 0, tzinfo=<Timezone [UTC]>), 4), (u'workflow', u'GERD_beam', datetime.datetime(2019, 5, 2, 0, 0, tzinfo=<Timezone [UTC]>), 4)])

Delete dataset, create dataset, create tables all work. 
When it came to beam transform, both transforms failed. If each transform is operated separately, each of them worked. 

Someone said on Piazza that it might be because airflow did not like the $GOOGLE_APPLICATION_CREDENTIALS. They changed the path and it worked. It did not work for our group 
Because each transform worked separately, I do not think there is a problem with the code. It might be a configuration problem. 
