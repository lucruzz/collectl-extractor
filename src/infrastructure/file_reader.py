import csv
from pathlib import Path

def read_job_file(inputpath: str, filename: str) -> list[list[str]]:
    filepath = inputpath + "/" + filename # concatena o caminho do diretório onde está localizado o arquivo com o nome do aruqivo
    fpath = Path(filepath)

    if not fpath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    with open(fpath, encoding='utf-8') as f:
        lines = f.readlines()
    
    return lines