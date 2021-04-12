import json
import pymongo
import inspect
from os import path

__all__ = ["JSONFileConfiguration", "MongoDBConfiguration"]


class Configuration:
    """
    Archetype of a configuration object, not meant to be used as a configuration manger on its own
    Meant to load and than manage to configuration values.
    The objective of inheriting objects is make loading parsing and accessing values of a configuration
    more streamlined
    """
    def __init__(self, needed_params: "A list"):
        self.needed_params = needed_params

    def __repr__(self):
        args_names = inspect.getfullargspec(self.__init__).args
        args = tuple(k + " : " + str(self.__dict__[k]) for k in args_names[1:])
        return str(self.__class__) + str(args)

    def __str__(self):
        string = tuple(f"{k}:{v}" for k, v in self.params.items())
        return str(string)


class JSONConfiguration(Configuration):
    """
    A prototype for all JSON file type configuration meant to hold the parsing function of the JSON file
    Not meant for independent use
    """
    def parse_json(self, config_content):
        if list(config_content.keys()) == self.needed_params:
            if len(self.needed_params) < len(config_content.keys()):
                raise Exception(f"Too few Parameters, given {list(config_content.keys())}, expected {self.needed_params}")
            elif len(self.needed_params) > len(config_content.keys()):
                raise Exception(f"Too many Parameters, given {list(config_content.keys())}, expected {self.needed_params}")
            else:
                self.params = config_content
        else:
            raise Exception(f"Parameters are missing, given {list(config_content.keys())}, expected {self.needed_params}")
        for k, v in self.params.items():
            self.__dict__[k] = v


class JSONFileConfiguration(JSONConfiguration):
    """
    A configuration stored in a simple JSON file in the windows file system
    EXP: {key1:value, key2:value, key3:value, key4:{keyA:value, keyB:value}}
    """
    def __init__(self, needed_params: "A list", file_path: "A string" = ".\\Configuration.json"):
        super().__init__(needed_params)
        self.file_path = file_path
        if path.isfile(file_path):
            with open(file_path, mode="r") as config_file:
                config_content = json.loads(config_file.read())
            self.parse_json(config_content)
        else:
            raise FileExistsError("Configuration file path provided does not exists")


class MongoDBConfiguration(JSONConfiguration):
    """
    A configuration stored in a simple JSON file in a MongoDB
    EXP: {key1:value, key2:value, key3:value, key4:{keyA:value, keyB:value}}
    """
    def __init__(self, needed_params: "A list", connection_url, database, collection):
        super().__init__(needed_params)
        self.connection = pymongo.MongoClient(connection_url)
        self.database = self.connection[database]
        self.collection = self.database[collection]
        config_file = self.collection.find_one()
        if config_file:
            config_content = json.loads(config_file)
            self.parse_json(config_content)
        else:
            raise FileExistsError("Configuration file location provided does not exists")


if __name__ == '__main__':
    pass
