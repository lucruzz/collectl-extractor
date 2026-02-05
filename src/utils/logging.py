colors = {
    'red': '\033[0;31m',
    'green': '\033[0;32m',
    'white': '\033[1;37m',
    'close': '\033[0m'
}

def log_error(msg: str) -> None:
    print(f"{colors['red']}[!] {msg}{colors['close']}")

def log_info(msg: str) -> None:
    print(f"{colors['green']}[+]{colors['close']} {colors['white']}{msg}{colors['close']}")