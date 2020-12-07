import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# DoFn to perform on each element in the input PCollection.
class Indicator(beam.DoFn):
    
    # change 29 columns of years to one column named 'Year'
    # which has 29 rows representing each year
    def process(self, element):
        record = element
        Country_Code = record.get('Country_Code')
        Indicator_Code = record.get('Indicator_Code')
        row = []
        
        for i in range (29):
            each_row = {}
            Year = 1990+i
            Value = record.get('Y'+str(1990+i))
            each_row['Country_Code'] = Country_Code
            each_row['Indicator_Code'] = Indicator_Code
            each_row['Year'] = Year
            each_row['Value'] = Value
            row.append(each_row)
            
        return row


PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset_1.SDGInfo WHERE Indicator_Code = "EN.ATM.PM25.MC.M3" LIMIT 100'))

    # write PCollection to log file
    query_results | 'Write to file' >> WriteToText('input_1.txt')

    # apply a ParDo to the PCollection
    indicator_pcoll = query_results | 'Direct' >> beam.ParDo(Indicator())


    # write PCollection to a file
    indicator_pcoll | 'Write to File' >> WriteToText('output_1.txt')
    
    # make BQ records    
    qualified_table_name =  PROJECT_ID + ':dataset_1.SDGdata_PM2_5'
    table_schema = 'Country_Code:STRING,Indicator_Code:STRING,Year:INTEGER,Value:FLOAT'
    
    indicator_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))


