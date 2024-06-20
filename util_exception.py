class WorkException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self.file_name = args[0]
        self.break_row = args[1]
        self.ex = args[2]
    
    def print_error(self):
        print(f'[ Error ] Error occurs, file: {self.file_name}, row: {self.break_row}, info: {self.ex}')

class SQLException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self.sql = args[0]
    
    def print_error(self):
        print(f'[ Error ] SQL error: {self.sql}')