import re
from datetime import datetime, timedelta
from typing import List
import subprocess
from pathlib import Path
from collections import defaultdict
import csv


def expand_nodelist(nodelist_str: str) -> List[str]:
    """
    Expande uma string no formato Slurm (ex: sdumont[1221-1225,1231,1331-1335])
    para uma lista de nomes completos (ex: ["sdumont1221", "sdumont1222", ...]).
    """
    match = re.match(r"([a-zA-Z0-9\-]+)\[(.+)\]", nodelist_str)

    if not match:
        return [nodelist_str]

    prefix, content = match.groups()

    nodes = []

    for part in content.split(","):
        if "-" in part:
            start, end = part.split("-")

            width = len(start)  # pra preserva os zeros à esquerda

            for i in range(int(start), int(end) + 1):
                nodes.append(f"{prefix}{str(i).zfill(width)}")
        else:
            nodes.append(f"{prefix}{part}")

    return nodes

# jobid, status, nodelist, jobstart, jobend 
def expand_nodelist_for_all(datajobs: list):
    data = []
    for job in datajobs:
        nodes = job[2]
        nodenamelist = expand_nodelist(nodes)
        nodelist = ",".join(nodenamelist)
        data.append(
            [
                job[0],     # jobid
                job[1],     # status
                nodelist,   # nodelist
                job[3],     # jobstart
                job[4]      # jobend
            ]
        )
    return data

def parse_time(timestr: str) -> datetime:
    return datetime.strptime(timestr, "%Y-%m-%d %H:%M:%S")

def build_node_usage(datajobs: list) -> dict:
    """
    Constrói um dicionário com os nós e os jobs que rodaram neles.
    Exemplo: { "sdumont1000": [(jobid, start, end), ...], ... }
    """
    usage = {}
    for job in datajobs:
        if job[1] == 'COMPLETED':
            jobid = job[0]
            start = parse_time(str(job[3]))
            end = parse_time(str(job[4]))
            nodes = job[2].split(',')
            for node in nodes:
                usage.setdefault(node, []).append((jobid, start, end))
    return usage

def intervals_overlap(a_start, a_end, b_start, b_end) -> bool:
    """Verifica se dois intervalos [a_start, a_end] e [b_start, b_end] se sobrepõem"""
    return not (a_end <= b_start or b_end <= a_start)

def verify_node_overlap(datajobs: list) -> list:
    """
    Verifica se há jobs sobrepostos, ou seja, que tenham executado no mesmo tempo no mesmo nó computacional.
    """
    usage = build_node_usage(datajobs)
    valid = []
    not_valid = []

    for job in datajobs:
        jobid = job[0]
        start = parse_time(str(job[3]))
        end = parse_time(str(job[4]))
        nodes = job[2].split(',')
        tmp = [jobid, nodes, start, end]

        is_valid = True
        for node in nodes:
            for other_jobid, ostart, oend in usage[node]:
                if other_jobid == jobid:
                    continue
                if intervals_overlap(start, end, ostart, oend):
                    is_valid = False
                    not_valid.append(tmp)
                    break
            if not is_valid:
                break

        if is_valid:
            valid.append(tmp)

    return valid, not_valid

def get_nodes_with_io_registered(year: str, month_start: str, month_end: str, schema: str, inputs_collectl: str, outputs_collectl: str) -> int:
    inputs_collectl += "/" + year
    result = subprocess.run(
        ['bash', './src/scripts/xtransforme_collectl.sh', year, month_start, month_end, schema, inputs_collectl, outputs_collectl], 
        # capture_output=True, # descomentar para permitir que o output do script bash NÃO apareça
        text=True
    )
    return result.returncode # 0 é sucesso

def io_leaf():
    return {"ost": False, "mdt": False, "tab": False}

def prettify_dict(d):
    if isinstance(d, dict):
        return {k: prettify_dict(v) for k, v in d.items()}
    return d

def organize_io_files(register_io_files: list[Path]) -> dict:
    # io_data = defaultdict(year_dict)
    io_data = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(io_leaf)
            )
        )
    )
    
    for myfile in register_io_files:
        with open(str(myfile)) as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                type, nodename, date = row[0], row[1], row[2]
                if row[3] != '':
                    type = row[3]

                date_object = datetime.strptime(date, "%Y%m%d")
                year, month, day = date_object.year, f"{int(date_object.month):02d}", f"{int(date_object.day):02d}"
                io_data[nodename][year][month][day][type] = True
    
    return prettify_dict(io_data)

def iter_days(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def validate_io_for_jobs(register_io_files: list[Path], valids: list, notvalids: list):
    """
    Essa função valida se de fato os jobs que foram considerados válidos para processamento possuem I/O registrado.
    A ideia é que essa função verifique não só se o nó computacional tem nome registrado, mas se o I/O foi registrado o dia em que o job estava executando.
    
    :param register_io_files: Lista com os caminhos dos arquivos (io_<ANO>_<MES>.csv) que possuem I/O coletado pelo collectl.
    :type register_io_files: list[Path]
    :param valids: Lista de jobs que foram considerados válidos (sem sobreposição e com possível I/O registrado)
    :type valid: list
    :param notvalids: Lista de jobs inválidos que será preenchida com os jobs que não tiverem I/O registrado.
    :type notvalid: list
    """

    iodata = organize_io_files(register_io_files)

    newvalids = []

    # adicionando valor para teste
    # valids.append([888888, ['sdumont2nd2018'], parse_time('2026-01-30 10:54:10'), parse_time('2026-02-01 11:07:02'), 1])

    for job in valids:
        # print(job[0], job[1], job[2], job[3], job[4])
        nodelist = job[1]
        startday = job[2]
        endday = job[3]

        job_with_full_io_registered = True

        for node in nodelist:
            if node not in iodata:
                job_with_full_io_registered = False
                break

            for day in iter_days(startday, endday):
                year = day.year
                month = f"{day.month:02d}"
                d = f"{day.day:02d}"

                try:
                    io_flags = iodata[node][year][month][d]

                    # if not any(io_flags.values()):
                    if not io_flags.get("ost", False):
                        job_with_full_io_registered = False
                        break
                
                except KeyError:
                    job_with_full_io_registered = False
                    break
            
            if not job_with_full_io_registered:
                break

        
        if not job_with_full_io_registered:
            jobid, nodelist, start, end, io = job[0], job[1], job[2], job[3], 0
            notvalids.append([jobid, nodelist, start, end, io])
        else:
            newvalids.append(job)

    return newvalids, notvalids


def get_nodeset(nodeset: set, inputfile: str) -> set:
    with open(inputfile) as f:
        for node in f:
            nodeset.add(node.strip().split(',')[1])
    return nodeset

def verify_io_for_jobs(datajobs: list, inputdir: str, year: str) -> list:
    """
    O objetivo dessa função é listar para quais jobs sem sobreposição, da lista recebida (datajobs), existe I/O registrado pelo collectl.

    Args:
        list: lista de jobs sem overlap.
        str: diretório de onde estão os arquivos arquivo dirs_YEAR_MONTH e io_YEAR_MONTH.

    Returns:
        list: Retorna duas listas, a primeira que são todos os jobs com I/O registrado e, a segunda, com os jobs que não tiveram registro.
    """

    p = Path(inputdir + "/" + year)
    io_nodes_registered = list(p.glob('io_*'))
    nodeset = set()
    for f in io_nodes_registered:
        nodeset = get_nodeset(nodeset, str(f))
    # columns = ['jobid', 'nodelist', 'jobstart', 'jobend', 'io']
    valid_data = []
    not_valid_data = []

    for job in datajobs:
        valid = 1
        nodelist = [node for node in job[1]]
        for node in nodelist:
            if node not in nodeset:
                valid = 0
            
            if not valid:
                break

        jobid, nodelist, start, end, io = job[0], job[1], job[2], job[3], valid

        if not valid:        
            not_valid_data.append([ jobid, nodelist, start, end, io ])
        else: 
            valid_data.append([ jobid, nodelist, start, end, io ])

    v, notv = validate_io_for_jobs(io_nodes_registered, valid_data, not_valid_data)
    return v, notv