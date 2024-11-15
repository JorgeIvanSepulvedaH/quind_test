import time
import tracemalloc
from functools import wraps

class Timer:
    def __init__(self, log):
        self.logger = log
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            self.logger.info(f"El método {func.__name__} tardó {execution_time:.4f} segundos en ejecutarse.")
            return result
        return wrapper

class Memory:
    def __init__(self, log):
        self.logger = log
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tracemalloc.start()
            memoria_inicial, _ = tracemalloc.get_traced_memory() 
            resultado = func(*args, **kwargs)
            memoria_final, _ = tracemalloc.get_traced_memory()
            uso_memoria = memoria_final - memoria_inicial
            tracemalloc.stop()
            self.logger.info(f"Uso de memoria de '{func.__name__}': {uso_memoria / 1024:.2f} KB")
            return resultado
        return wrapper

if __name__ == "__main__":
    # Logger object
    import logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    # Example
    @Timer(logger)
    @Memory(logger)
    def ejemplo_funcion():
        time.sleep(1)  

    # Execute decored function
    ejemplo_funcion()