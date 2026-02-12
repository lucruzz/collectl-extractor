from argparse import Namespace

def apply_config_from_file(parser: Namespace) -> Namespace:
    import configparser
    
    config = configparser.ConfigParser()
    config.read(parser.config_file)

    # variaveis sobre o tipo de dado a ser processado
    parser.type = config.get('Processing', 'processing_type')

    parser.action = config.get('Processing', 'processing_action')
    
    parser.year = config.get('Processing', 'processing_year')
    parser.month_start = config.get('Processing', 'processing_month_start')
    parser.month_end = config.get('Processing', 'processing_month_end')

    if parser.action == 'populate' or parser.action == 'update':
        # variaveis sobre o job
        if parser.type == 'job':
            parser.input_file = config.get('Job', 'job_input_file')
            # output_file = config.get('Job', 'job_output_file')
        else:
            parser.input_file = config.get('IO', 'io_input_file')
            # output_file = config.get('IO', 'io_output_file')

    # variaveis dos diretórios de saída e entrada de arquivos
    parser.input = config.get('Directory', 'directory_inputs')
    parser.output = config.get('Directory', 'directory_outputs')
    parser.inputs_collectl = config.get('Directory', 'directory_inputs_collectl')
    parser.outputs_collectl = config.get('Directory', 'directory_outputs_collectl')

    # variaveis do database
    parser.user = config.get('Database', 'database_user')
    parser.password = config.get('Database', 'database_password')
    parser.schema_db = config.get('Database', 'database_schema')
    parser.dbname = config.get('Database', 'database_name')
    parser.host = config.get('Database', 'database_host')
    parser.port = int(config.get('Database', 'database_port'))
    parser.update_db = False if config.get('Database', 'database_update_db') != "True" else True
    parser.populate_db = False if config.get('Database', 'database_populate_db') != "True" else True
    parser.columns_db = config.get('Database', 'database_columns_db')
    
    return parser