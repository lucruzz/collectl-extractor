from argparse import Namespace
import utils.code as code


def job_validate_required_fields(parser: Namespace) -> int:

    # verifica se o parametro de população ou atualização do banco foi configurado
    if parser.populate_db or parser.update_db:
        # se tiver sido configurado as credenciais do usuário para acessar o banco devem ser informadas
        if not parser.user or not parser.password:
            # se não tiverem sido informadas as credenciais para acessar o banco retorna falha
            return code.ERROR_NO_USER_OR_PASSWORD
    
    # caso não dê problema, continua a execuçao para verificar os demais parâmetros
    if (parser.populate_db or parser.update_db) and not parser.schema_db:
        return code.ERROR_NO_SCHEMA_DECLARED
    
    # verifica se o arquivo de input dos jobs que será parseado para popular o banco foi indicado
    if (parser.populate_db or parser.update_db) and not parser.input_file:
        # encerra indicando falha devido a falta do arquivo (dos jobs)
        return code.ERROR_NO_INPUT_CSV_FILE

    # verifica se é atualização e se a(s) coluna(s) para ser(em) atualizada(s) não foi/foram informada(s)
    if parser.update_db and not parser.columns_db:
        # se a(s) coluna(s) não tiver(em) sido indicada(s) então retorna erro
        return code.ERROR_NO_COLUMN_INDICATED
    
    return code.SUCCESS