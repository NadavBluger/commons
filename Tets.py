from commons.loggers import TextFileLogger
import threading
import logging

log = TextFileLogger("DEBUG")

i = 0
while i < 100:
    i += 0.00000000001
    log.log(f"fuck{i}", "INFO")


