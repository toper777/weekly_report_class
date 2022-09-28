import ctypes

import colorama

kernel32 = ctypes.windll.kernel32
kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
print(f'{colorama.Fore.GREEN}GREEN TEXT{colorama.Fore.RESET}')
print(f'{colorama.Fore.RED}RED TEXT{colorama.Fore.RESET}')
