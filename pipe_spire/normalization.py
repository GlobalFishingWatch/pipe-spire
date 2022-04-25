"""
Executes a query to normalizes the Spire AIS messages

This script will do:
1- Applies the jinja templating to the Spire normalized query.
2- Creates the destination table in case it doesnt exist.
3- Run the query and save results in destination table.
"""

from datetime import datetime, timedelta
from jinja2 import Environment, FileSystemLoader
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import argparse, json, logging, re, time


logging.basicConfig(level=logging.INFO)

def create_table_if_not_exists(client, destination_table_ref, description):
    """Creates table if it does not exists

    :param client: Client of BQ.
    :type client: BigQuery.Client
    :param destination_table_ref: Reference of a Table in BQ.
    :type destination_table_ref: BigQuery.TableReference
    :param description: The table description.
    :type description: str.
    """
    logging.info('Checking existence of table.')
    try:
        table = client.get_table(destination_table_ref) #API request
        logging.info(f'  Table {destination_table_ref} already exists.')
    except NotFound:
        with open('./assets/schemas/spire-normalize-schema.json') as sch:
            schema = json.load(sch)
        table = bigquery.Table(destination_table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_ = bigquery.TimePartitioningType.DAY,
            field = "timestamp",  # name of column to use for partitioning
        )
        table.clustering_fields = ["msgid","ssvid"]
        table = client.create_table(table)
        logging.info(f'  Table {destination_table_ref} created with specific schema.')

    table.description = description
    client.update_table(table, ['description'])
    logging.info('  Table description updated.')

def get_version():
    with open('./setup.py') as s:
        ver = re.search('(\d\.\d\.\d)', s.read()).group(0)
    return ver

def build_description(source, destination, date_range):
    return f"""   * Pipeline: pipe-spire {get_version()}
    * Source: {source}
    * Command: ./main.py normalize -i {source} -o {destination} -dr {date_range}"""

date_range_parse = lambda x : list(map(lambda dt: datetime.strptime(dt,'%Y-%m-%d'), x.split(',')))

def run_query(client, query, destination_table, dry_run=False):
    # Configures the job
    job_config = bigquery.QueryJobConfig(
        dry_run = dry_run,
        use_query_cache = False,
        priority = bigquery.QueryPriority.BATCH,
        use_legacy_sql = False,
        write_disposition = 'WRITE_TRUNCATE',
        time_partitioning = bigquery.TimePartitioning(
            type_ = bigquery.TimePartitioningType.DAY,
            field = "timestamp",  # name of column to use for partitioning
        ),
        clustering_fields = ["msgid","ssvid"],
        destination = f'{client.project}.{destination_table}'
    )

    return client.query(query, job_config=job_config)  # Make an API request.

def normalize(args):
    parser = argparse.ArgumentParser(description='Normalizes the Spire AIS messages.')
    parser.add_argument('-i','--source_table', help='The BQ source table (Format str, ex: datset.table).', required=True)
    parser.add_argument('-o','--destination_table', help='The BQ destination table (Format str, ex: datset.table).', required=True)
    parser.add_argument('-dr','--date_range', help='The date range to be processed (Format str YYYY-MM-DD[,YYYY-MM-DD]).', required=True)
    parser.add_argument('-rf','--reduce_factor', help='The reduce_factor (Format float).', required=False, default='')

    args_parsed = parser.parse_args(args)

    start_time = time.time()

    description = build_description(args_parsed.source_table, args_parsed.destination_table, args_parsed.date_range)
    date_ranges = date_range_parse(args_parsed.date_range)
    if len(date_ranges) == 1:
        date_ranges.append(date_ranges[0] + timedelta(days=1))
    date_from, date_to = date_ranges

    # Apply template
    env = Environment(loader=FileSystemLoader('./assets/queries/'))
    template = env.get_template('spire-normalize.sql.j2')

    with open('./assets/shiptypes.json') as st:
        shiptypes = json.load(st)

    date_from_dash = date_from.strftime('%Y-%m-%d')

    query = template.render(
        shiptypes,
        dt = date_from_dash,
        source = args_parsed.source_table,
        reduce_factor = args_parsed.reduce_factor
    )
    print('===========')
    print('Query: ', query)
    print('===========')

    #Creates partitioned table
    project_id, dataset_table = args_parsed.destination_table.split(':')
    dataset_id, table_id = dataset_table.split('.')

    client = bigquery.Client(project=project_id)

    destination_dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    destination_table_ref = destination_dataset_ref.table(table_id)
    create_table_if_not_exists(client, destination_table_ref, description)

    # Run the calc of how much bytes will spend
    query_job = run_query(client, query, f'{dataset_id}.{table_id}', True)
    logging.info(f'This query will process {query_job.total_bytes_processed} bytes.')

    # Run query
    query_job = run_query(client, query, f'{dataset_id}.{table_id}')
    logging.info(f'Job {query_job.job_id} is currently in state {query_job.state}')

    query_job.result() # Wait for the job to complete.


    ### ALL DONE
    logging.info(f'All done, you can find the output ({date_from.strftime("%Y%m%d")}-{date_to.strftime("%Y%m%d")}): {args_parsed.destination_table}')
    logging.info(f'Execution time {(time.time()-start_time)/60} minutes')
