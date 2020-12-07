import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# DoFn to perform on each element in the input PCollection.
class Indicator(beam.DoFn):
    def process(self, element):
        record = element
        Country_Code = record.get('Country_Code')
        Indicator_Code = record.get('Indicator_Code')

        sum_value = 0
        count = 0
        for i in range (29):
            value = record.get('Y'+str(1990 + i))
            if value is not None:
                sum_value = value + sum_value
                count = count + 1
                
        # take care of empty rows
        if count == 0:
            avg_val = None
        else:
            avg_val = sum_value/count

        aver = []
        ave = {}
        ave['Country_Code'] = Country_Code
        ave['Indicator_Code'] = Indicator_Code
        ave['Average_value'] = avg_val
        aver.append(ave)

        return aver


PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset_1.SDGInfo'))

    # write PCollection to log file
    query_results | 'Write to file' >> WriteToText('input.txt')

    # apply a ParDo to the PCollection
    indicator_pcoll = query_results | 'Average Indicator' >> beam.ParDo(Indicator())


    # write PCollection to a file
    indicator_pcoll | 'Write to File' >> WriteToText('output.txt')

    # make BQ records    
    qualified_table_name =  PROJECT_ID + ':dataset_1.SDGdata_average'
    table_schema = 'Country_Code:STRING,Indicator_Code:STRING,Average_value:FLOAT'
    
    indicator_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))



