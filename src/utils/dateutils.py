import calendar

def build_date_end(year: str, month: str) -> str:
    day = calendar.monthrange(int(year), int(month))[1]
    return f"{year}-{int(month):02d}-{day}"

def build_date_start(year: str, month: str) -> str:
    return f"{year}-{int(month):02d}-01"
