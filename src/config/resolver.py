from argparse import Namespace
from .loader import apply_config_from_file
from .schema import job_validate_required_fields

def resolve_configuration(parser: Namespace) -> Namespace:
    if parser.config_file:
        parser = apply_config_from_file(parser)
        
    match parser.type:
        case 'job':
            job_validate_required_fields(parser)
        case 'io':
            pass
        case _:
            raise ValueError('Erro de resolver.py: parâmetros faltantes ou inválidos!')
    
    return parser