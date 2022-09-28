import ctypes


class Colors:
    """
    Содержит кодировки цветов для консольного вывода
    """

    PURPLE = u'\x1b[95m'
    CYAN = u'\x1b[96m'
    DARKCYAN = u'\x1b[36m'
    BLUE = u'\x1b[94m'
    GREEN = u'\x1b[92m'
    YELLOW = u'\x1b[93m'
    RED = u'\x1b[91m'
    BOLD = u'\x1b[1m'
    UNDERLINE = u'\x1b[4m'
    END = u'\x1b[0m'
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
