from argparse import ArgumentParser, Namespace

def parse_arguments() -> Namespace:
    parser = ArgumentParser(
        prog='Collectl Extractor',
        description='Collectl Extractor is an I/O data collector. It is able to associate I/O information with the exact job that generated it.',
        epilog='For more information, see the README.',
    )

    # arquivo de configuração para substituir as configurações via CLI.
    parser.add_argument(
        '-C', '--columns-db', 
        type=str, 
        help='Columns to be updated on database.', 
        default='inputs'
    )

    # arquivo de configuração para substituir as configurações via CLI.
    parser.add_argument(
        '-c', '--config-file', 
        type=str, 
        help='Configuration file.'
    )

    # nome do bd.
    parser.add_argument(
        '-D', '--dbname', 
        type=str, 
        help='Database name.',
        default='aiio_db'
    )
    
    
    # diretório onde estão localizados os arquivos de entrada.
    parser.add_argument(
        '-i', '--input', 
        type=str, 
        help='Full directory path to read/extrac the files.', 
        default='inputs'
    )

    # arquivo csv que será utilizado para ler as informações dos jobs para serem cadastrados no BD.
    parser.add_argument(
        '-I', '--input-csv', 
        type=str, 
        help='File CSV.'
    )
    
    # Diretório onde serão gerados os arquivos de saída, se houver.
    parser.add_argument(
        '-o', '--output', 
        type=str, 
        help='Directory where outputs will be generated.', 
        default='outputs'
    )

    # Senha do usuário cadastrado no banco de dados, dono do BD.
    parser.add_argument(
        '-P', '--password', 
        type=str, 
        help='Password to access database.'
    )

    # Parâmetro para indicar se o banco de dados deve ser populado. Padrão é "False", ou seja, não deve ser populado.
    parser.add_argument(
        '-p', '--populate-db', 
        type=bool, 
        help='Populates database (only with slurm jobs).',
        default=False
    )

    # Parâmetro para indicar se o banco de dados deve ser populado. Padrão é "False", ou seja, não deve ser populado.
    parser.add_argument(
        '-s', '--schema-db', 
        type=str, 
        help='Used to indicate the PostgreSQL database schema that will be used.'
    )
    
    # O tipo de dado que será processado. Se for "io", então é dado do collectl. Se for "job", então é dado do slurm.
    parser.add_argument(
        '-t', '--type', 
        type=str, 
        help='Type of data input file: \"io\" for I/O data or \"job\" for Slurm job data.'
    )

    # Se o banco deve ser atualizado.
    parser.add_argument(
        '-u', '--update-db', 
        type=bool, 
        help='Update database (only with slurm jobs).',
        default=False
    )

    # Usuário cadastrado no banco, dono do BD.
    parser.add_argument(
        '-U', '--user', 
        type=str, 
        help='Username to access database.'
    )

    return parser.parse_args()