import time
from alive_progress import alive_bar

def long_task(duration=5):
    with alive_bar(duration) as bar:
        for i in range(duration):
            time.sleep(1)
            bar()

def loading(funcion,mensaje,*args):
    result = True
    with alive_bar(1, title=mensaje) as bar:
        result = funcion()
        bar()
    print("Tarea completada.")
    return result