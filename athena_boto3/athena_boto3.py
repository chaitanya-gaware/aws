class athena:
    aws_key_id = os.environ.get('aws_key_id')
    aws_key = os.environ.get('aws_key')
    
    def __init__(self, region_name):
        self.region_name = region_name
        self.athena_client = boto3.client('athena', 
                                   aws_access_key_id = athena.aws_key_id, 
                                   aws_secret_access_key = athena.aws_key,
                                   region_name = self.region_name)
        
    def run_query(self, query, database, catalog, output):
        self.query = query
        self.database = database
        self.catalog = catalog
        self.output = output
        try:
            res = self.athena_client.start_query_execution(
                QueryString = self.query,
                QueryExecutionContext = {
                    'Database':self.database,
                    'Catalog':self.catalog
                }
            ResultConfiguration = {
                'OutputLocation':self.output
            }  
            )
            query_execution_id = res['QueryExecutionId']
            return query_execution_id
        except Exception as e:
            print(f'Error running query: {e}')
            return None
athena_instance = athena(region_name='your_region_name')
query_execution_id = athena_instance.run_query(
    query='SELECT * FROM your_table',
    database='your_database_name',
    catalog='your_catalog',
    output='s3://your-athena-query-results-bucket/'
)  