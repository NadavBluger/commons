from datetime import datetime


def log_func_args(logger):
    def dec_function(function):
        def wrapper_func(*args, **kwargs):
            logger.log(f"The function {function.__name__} was called with the following argument: {args}, {kwargs}")
            return function(*args, **kwargs)
        return wrapper_func()
    return dec_function()


def log_func_runtime(logger):
    def dec_func(function):
        def wrapper_func(*args, **kwargs):
            start_time = datetime.now()
            result = function(*args, **kwargs)
            logger.log(f"The Function {function.__name__} took {datetime.now()-start_time} to run")
        return wrapper_func()
    return dec_func()
