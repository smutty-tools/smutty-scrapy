import os


class SmuttyException(Exception):
    pass


class IntegerStateFile:

    def __init__(self, file_name):
        self.file_name = file_name

    def set(self, value):
        with open(self.file_name, "wt") as file_obj:
            file_obj.write("{0}".format(value))

    def get(self):
        try:
            with open(self.file_name, "rt") as file_obj:
                return int(file_obj.read())
        except FileNotFoundError as exception:
            return None

    def delete(self):
        os.remove(self.file_name)
