import re
from datetime import datetime
from typing import List
import subprocess
from pathlib import Path


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

def get_nodeset(nodeset: set, inputfile: str) -> set:
    with open(inputfile) as f:
        for node in f:
            nodeset.add(node.strip())
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

    return valid_data, not_valid_data