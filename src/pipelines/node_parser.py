import re
from datetime import datetime
from typing import List


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

        is_valid = True
        for node in nodes:
            for other_jobid, ostart, oend in usage[node]:
                if other_jobid == jobid:
                    continue
                if intervals_overlap(start, end, ostart, oend):
                    is_valid = False
                    not_valid.append(job)
                    break
            if not is_valid:
                break

        if is_valid:
            valid.append(job)

    return valid, not_valid

