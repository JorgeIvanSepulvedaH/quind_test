import logging

class Logger:
    def __init__(self, logger_name, log_file=None, level=logging.DEBUG):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(level)
        self.logger.propagate = False  # Evitar duplicación de logs

        if log_file:
            handler_file = logging.FileHandler(log_file)
            handler_file.setLevel(level)
            handler_file.setFormatter(self._crear_formato())
            self.logger.addHandler(handler_file)

        handler_console = logging.StreamHandler()
        handler_console.setLevel(level)
        handler_console.setFormatter(self._crear_formato())
        self.logger.addHandler(handler_console)

    def _crear_formato(self):
        return logging.Formatter('%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s')

    def debug(self, mensaje):
        self.logger.debug(mensaje)

    def info(self, mensaje):
        self.logger.info(mensaje)

    def warning(self, mensaje):
        self.logger.warning(mensaje)

    def error(self, mensaje):
        self.logger.error(mensaje)

    def critical(self, mensaje):
        self.logger.critical(mensaje)
