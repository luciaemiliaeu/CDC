import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import csv
import pandas as pd

PWD = os.getcwd()
WATCH_DIRECTORY = PWD + "\input_data"
SAVE_DIRECTORY = PWD + "\output_data"


class OnMyWatch:

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, WATCH_DIRECTORY, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except:
            self.observer.stop()
        self.observer.join()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_created(event):
        # O código a seguir escuta um evento de criação de arquivo na pasta input_data.
        # Caso um evento seja detectado, faz-se a comparação entre os dados inseridos(event.src_path) e o arquivo
        # de dados mais atualizados (updated.csv).
        # O arquivo updated.csv é então atualizado inserindo as linhas novas (onde a coluna 'op' == i) e atualiando as
        # modificadas (onde a coluna 'op' == u).
        # Ao final, os dados do novo evento são adicionadoa ao arquivo history.csv.

        if event.event_type == 'created':

            source = pd.read_csv(event.src_path, header=[0], index_col=[1])
            target = pd.read_csv(SAVE_DIRECTORY + '\\updated.csv', header=[0], index_col=[1])

            if not target.empty:

                inserts = source[source['op'] == 'i']
                target = pd.concat(
                    [target.reset_index()[['op', 'customer_id', 'balance', 'create_timestamp', 'update_timestamp']],
                     inserts.reset_index()[['op', 'customer_id', 'balance', 'create_timestamp', 'update_timestamp']]]).\
                    set_index('customer_id')

                updates = source[source['op'] == 'u']
                target.update(updates)

            else:
                target = pd.concat(
                    [target.reset_index()[['op', 'customer_id', 'balance', 'create_timestamp', 'update_timestamp']],
                     source.reset_index()[['op', 'customer_id', 'balance', 'create_timestamp', 'update_timestamp']]])

            target.reset_index()[['op', 'customer_id', 'balance', 'create_timestamp', 'update_timestamp']] \
                .to_csv(SAVE_DIRECTORY + "/updated.csv", index=False)

            with open(SAVE_DIRECTORY + '/history.csv', 'a', newline='') as history:
                writer = csv.writer(history)
                writer.writerows(source.reset_index()[
                                     ['op', 'customer_id', 'balance', 'create_timestamp', 'update_timestamp']].values)

if __name__ == '__main__':
    # Checa se os diretórios existem, senão cria.
    if not os.path.isdir(WATCH_DIRECTORY):
        os.makedirs(WATCH_DIRECTORY)
    if not os.path.isdir(SAVE_DIRECTORY):
        os.makedirs(SAVE_DIRECTORY, exist_ok=True)

    watch = OnMyWatch()
    watch.run()
