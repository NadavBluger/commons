"""
Different Logger classes.

Logger- archetype for a logger, not ment to be used on its own

TextFileLogger - log directly into a txt file

MongoDBLogger - logs into a MongoDB
"""
import os
import pymongo
from datetime import datetime

__all__ = ["TextFileLogger", "MongoDBLogger"]


class Logger:
    """
    A Logger archetype not meant to be used on its own
    """
    levels = {"DEBUG": 5, "INFO": 4, "WARN": 3, "ERROR": 2, "FATAL": 1}

    def __init__(self, level):
        level = level.upper()
        if level in Logger.levels.keys():
            self.level = Logger.levels[level]
        else:
            raise ValueError(f"Expected level to be one of {list(Logger.levels.keys())}")

    def log(self, message, level):
        raise NotImplementedError


class TextFileLogger(Logger):
    """
    A logger whose messages are written to text files.
    It supports splitting the logged messages to different severity files along with writing all messages to
    an all severity inclusive file.
    Maneges log files sizes and amount of backlog files to be kept
    """
    def __init__(self, level, dir_: 'str' = None, max_file_size: 'In Kb' = 1000, *, split_log=False, files_amount=100):
        """
        :param level:  The severity level of the message
        :param dir_: Dir where logs are to be stored, by default will be 'Logs' in current running dir
        :param max_file_size: The largest size to which a log will be allowed to grow before
        getting backlogged and replaced
        :param split_log: Whether to split messages to severity file as well as to the inclusive file
        :param files_amount: Maximum amount of files to be kept in the backlog
        """
        super(TextFileLogger, self).__init__(level)
        self.dir = dir_ if dir_ else f"{os.getcwd()}\\Logs"
        self.split_log = split_log
        self.max_file_size = max_file_size * 1024
        self.files_amount = files_amount
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)

    def __repr__(self):
        return f"TextFileLogger({self.level},{self.dir},{self.max_file_size},{self.split_log},{self.files_amount})"

    def log(self, message, level):
        """
        Validates Log Level and checks to see if given level is enabled in current logger, if so loges the message
        :param message: Message to be printed to the log
        :param level: The severity level of the message
        :return:
        """
        level = level.upper()
        if level not in Logger.levels:
            raise ValueError(f"Expected level to be one of {Logger.levels.keys()}")
        if Logger.levels[level] <= self.level:
            if self.split_log:
                log_path = f"{self.dir}\\{level}"
                self._log_message(log_path, level, message, split=True)
            self._log_message(self.dir, level, message)

    def _log_message(self, path, level, message, split=False):
        """
        Creates logging dir and writes to the log
        :param path: Dir where logs are to be stored
        :param level: The severity level of the message
        :param message: Message to be printed to the log
        :param split: Whether called to write to split log or to inclusive log
        :return:
        """
        name = level if split else "log"
        if not os.path.exists(path):
            os.mkdir(path)
        with open(self.choose_file(name=name), mode="a")as file:
            file.write(f"{datetime.now()} {level}: {message} \n")

    def choose_file(self, name="log"):
        """
        Chooses the file to which the message will be written and id needed calls manage_old_files
        :param name: Name of the log file
        :return: A path to the chosen log file
        """
        if name == "log":
            file_path = f"{self.dir}\\{name}.txt"
        else:
            file_path = f"{self.dir}\\{name}\\{name}.txt"
        if os.path.exists(file_path):
            if os.path.getsize(file_path) < self.max_file_size:
                return file_path
            else:
                self.manage_old_files(name=name)
                return file_path
        else:
            return file_path

    def manage_old_files(self, name):
        """
        Handles creating new file when size limit it met and deleting older files when amount limit is met
        :param name: Name of the log file
        :return:
        """
        if name == "log":
            path = self.dir
        else:
            path = f"{self.dir}\\{name}"
        files = [f for f in os.listdir(path) if os.path.isfile(f"{path}\\{f}")]
        for file in files:
            if name not in file:
                files.pop(files.index(file))
        if len(files) == self.files_amount:
            os.remove(f"{path}\\{files[-1]}")
            files.pop()
        for i in range(len(files), 0, -1):
            os.rename(f"{path}\\{files[i-1]}", f"{path}\\{name}{i}.txt")


class MongoDBLogger(Logger):
    """
    A logger who writes log files (individual messages) to a MongoDB
    It supports splitting the logged messages to different severity collections along with writing all messages to
    an all severity inclusive collection.
    Maneges log files sizes and amount of backlog files to be kept
    """
    def __init__(self, level, connection_url, database, max_collection_size: 'In Kb' = 1000, *,
                 split_log=False, files_amount=10):
        """
        :param level: The severity level of the message
        :param connection_url: The connection URL to the MongoDB
        :param database: Database name within the MongoDB instance
        :param max_collection_size: The max size in Kb of the log files collections
        :param split_log: Whether to split messages to severity collection as well as to the inclusive collection
        :param files_amount: The max amount of files to be stored in a collection
        """
        super(MongoDBLogger, self).__init__(level)
        self.connection = pymongo.MongoClient(connection_url)
        self.database = self.connection[database]
        self.split_log = split_log
        self.file_amount = files_amount
        self.max_collection_size = max_collection_size * 1024

    def __repr__(self):
        return f"MongoDBLogger({self.level},{self.connection.address},{self.database}," \
               f"{self.max_collection_size},{self.split_log},{self.file_amount})"

    def log(self, message, level):
        """
        Validates Log Level and checks to see if given level is enabled in current logger, if so loges the message
        :param message: Message to be printed to the log
        :param level: The severity level of the message
        :return:
        """
        level = level.upper()
        if level not in Logger.levels:
            raise ValueError(f"Expected level to be one of {Logger.levels.keys()}")
        if Logger.levels[level] <= self.level:
            if self.split_log:
                collection = self.database[level]
                self.write_file(message, level, collection)
            collection = self.database["log"]
            self.write_file(message, level, collection)

    def write_file(self, message, level, collection):
        """
        Inserts lof file to given collection and than calls size and amount managers.
        :param message: Message to be written in log file
        :param level: Level of log message
        :param collection: Collection to insert into the log file
        :return:
        """
        collection.insert_one({"Timestamp1": datetime.now(), "Severity": level, "Message": message})
        self.manage_file_amount(collection)
        self.manage_collection_size(collection)

    def manage_file_amount(self, collection):
        """
        Deletes the oldest file from a give collection if said collection surpassed allowed amount of files
        :param collection:
        :return:
        """
        if collection.count_documents({}) > self.file_amount:
            self.delete_oldest_doc(collection)

    @staticmethod
    def delete_oldest_doc(collection):
        """
        Deletes the oldest file from a given collection
        :param collection: Collection from which to delete the oldest file
        :return:
        """
        collection_files = collection.find({}, {"Timestamp1": 1}).sort("Timestamp1")
        for x in collection_files.limit(1):
            collection.delete_one({"_id": x["_id"]})

    def manage_collection_size(self, collection):
        """
        Deletes oldest file from a collection if said collection surpass the allowed max size
        :param collection: the collection to mange
        :return:
        """
        name = collection.name
        size = self.database.command("collstats", name)["size"]
        if size > self.max_collection_size:
            self.delete_oldest_doc(collection)


if __name__ == '__main__':
    a = TextFileLogger("INFO")
    a.log("FUck", "DEBUG")
