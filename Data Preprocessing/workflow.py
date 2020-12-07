import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

###### SQL variables ###### 
raw_dataset = 'dataset_2'
new_dataset = 'workflow'
sql_cmd_start = 'bq query --use_legacy_sql=false '

sql_SEO = 'create table ' + new_dataset + '.SEO_workflow as select * ' \
           'from ' + raw_dataset + '.SEO ' \
       
sql_GERD = 'create table ' + new_dataset + '.GERD_workflow as select * ' \
            'from ' + raw_dataset + '.GERD_Series ' \

  
###### Beam variables ######          
LOCAL_MODE=1 # run beam jobs locally
DIST_MODE=2 # run beam jobs on Dataflow

mode=LOCAL_MODE

if mode == LOCAL_MODE:
    SEO_script = 'transform_SEO_single.py'
    GERD_script = 'transform_GERD_Series_single.py'
    
if mode == DIST_MODE:
    SEO_script = 'transform_SEO_cluster.py'
    GERD_script = 'transform_GERD_Series_cluster.py'

###### DAG section ###### 
with models.DAG(
        'workflow',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    ###### SQL tasks ######
                
    delete_dataset = BashOperator(
            task_id='delete_dataset',
            bash_command='bq rm -r -f workflow')
    
    create_dataset = BashOperator(
            task_id='create_dataset',
            bash_command='bq mk workflow')
                    
    create_SEO_table = BashOperator(
            task_id='create_SEO_table',
            bash_command=sql_cmd_start + '"' + sql_SEO + '"')
            
    create_GERD_table = BashOperator(
            task_id='create_GERD_table',
            bash_command=sql_cmd_start + '"' + sql_GERD + '"')
            
    
    ###### Beam tasks ######     
    SEO_beam = BashOperator(
            task_id='SEO_beam',
            bash_command='python /home/markjilonglin/airflow/dags/' + SEO_script)
            
    GERD_beam = BashOperator(
            task_id='GERD_beam',
            bash_command='python /home/markjilonglin/airflow/dags/' + GERD_script)
            
            
    transition = DummyOperator(task_id='transition')
            
    delete_dataset >> create_dataset >> [create_SEO_table, create_GERD_table] >> transition
    transition >> [SEO_beam, GERD_beam]
