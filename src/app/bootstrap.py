from cli.parser import parse_arguments
from config.resolver import resolve_configuration
from app.dispatcher import dispatch


def start() -> None:
    parser = parse_arguments()
    
    config = resolve_configuration(parser)

    dispatch(config)
