TYPES = {
    'HEADER': '\033[95m',
    'OKBLUE': '\033[94m',
    'OKCYAN': '\033[96m',
    'OKGREEN': '\033[92m',
    'WARNING': '\033[93m',
    'FAIL': '\033[91m',
    'BOLD': '\033[1m',
    'UNDERLINE': '\033[4m',
}
TYPE_END = '\033[0m'


class Logger:
    def __init__(self):
        self.type = None

    def set_log_type(self, type):
        if type in TYPES:
            self.type = type
        else:
            raise Exception('Unknown logging type {}'.format(type))

    def log(self, *args):
        if self.type is None or self.type not in TYPES:
            print(*args)
        else:
            strs = [str(o) for o in args]
            msg = ' '.join(strs)
            print('{}{}{}'.format(TYPES[self.type], msg, TYPE_END))
