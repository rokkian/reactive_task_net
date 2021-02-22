from json.decoder import JSONDecodeError
from os import stat_result
import redis
import logging
import random
import time
import json
import sys
from typing import Final

from redis.client import PubSub, Redis
"""
Questo è il master sincrono autorizzato base, il suo compito è:
    1) generare un task e notificarlo agli iscritti
    2) leggere le proposte degli iscritti disponibili
    3) scegliere l'iscritto che svolga il task
    4) leggere la notifica di completamento del task e recuperare il risultato
    5) Ripetere
"""

#HOST : Final = "localhost"
HOST : Final = "192.168.1.17"
PORT : Final = 6379
DB : Final = 0
MAX_EXECUTION_TIME : Final = 3000      #massimo tempo di esecuzione dei task (ms)
MAX_NETWORK_DELAY : Final = 150     #massimo ritardo di rete (ms)
REPUBLISH_DELAY : Final = 1/0.5         #quanti task pubblicare al secondo

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

class Task(object):     #viene serializzata in JSON e inviata
    def __init__(self, task_types, task_counter, master_name):
        super().__init__()
        self.task_type = task_types[random.randint(0,len(task_types)-1)]
        self.task_name = f"{self.task_type}_{task_counter}"
        self.a = random.randint(1, 100)
        self.b = random.randint(1, 100)
        self.result = None
        self.master_receiving_channel = master_name+self.task_name    # lo imposto come identificativo del task
        self.chosen_worker = ''

    def set(self, type, taskname, a, b, master_name ):
        self.task_type = type
        self.task_name = taskname
        self.a = a
        self.b = b
        self.master_receiving_channel = master_name + self.task_name

    def __repr__(self) -> str:
        return self.master_receiving_channel

class Master(object):
    def __init__(self, only_sum=False) -> None:
        super().__init__()
        self.master_name = f"master{random.randint(0, 1e4)}_"
        self.pool = redis.ConnectionPool(host=HOST, port=PORT, db=0)
        self.subscriber = redis.Redis(connection_pool=self.pool).pubsub()
        self.received_messages = []
        self.agent_set = set()
        self.task_counter = 0
        self.task_types = ["task+", "task-", "task*", "task/"]
        self.only_sum = only_sum
        self.reassign_task_counter = 0
        if (self.only_sum):
            self.task_types = ["task+"]
    
    def create_task(self) -> None:
        #pulizia dati task precedente
        if hasattr(self, 'active_task'):
            self.subscriber.unsubscribe(self.active_task.master_receiving_channel)
            logging.info(f"Dis-Iscritto da {self.active_task.master_receiving_channel}")
        self.received_messages = []

        #creazione nuovo task
        self.active_task = Task(self.task_types, self.task_counter, self.master_name)
        self.subscriber.subscribe(self.active_task.master_receiving_channel)
        logging.info(f"Iscritto a {self.active_task.master_receiving_channel}")
        self.task_counter+=1


    def publish_task(self) -> None:
        task_message_json = json.dumps(self.active_task.__dict__)
        r = redis.Redis(connection_pool=self.pool)
        n = r.publish(self.active_task.task_name, task_message_json)    #invio la key dove inserire il risultato e i numeri
        logging.info(f'\nTask published "{task_message_json}", on channel "{self.active_task.task_name}" received by {n} agents')

        while n == 0:
            time.sleep(REPUBLISH_DELAY)
            n = r.publish(self.active_task.task_name, task_message_json)    #invio la key dove inserire il risultato e i numeri
            logging.info(f'Task published "{task_message_json}" received by {n} agents')

    def receive_proposes_and_assign(self) -> str:
        #ricevo le proposte degli slave (canale)---------------------------------------------------------------------------------
        proposals = []

        while True:
            time.sleep(MAX_NETWORK_DELAY/1000)                                                       # attende messaggi dagli agenti (prova a ridurre)
            msg = self.subscriber.get_message(ignore_subscribe_messages=True, timeout=0.0)

            if msg is not None:
                self.received_messages.append(msg)

            while msg is not None:
                try:
                    if msg["channel"].decode('utf-8') == self.active_task.master_receiving_channel and msg['data'].decode("utf-8")[0]=="{":   #da controllare la condizione
                        data_dict = json.loads(msg['data'])
                        if data_dict['propose']==True and msg not in proposals:
                            proposals.append(msg)
                        else:
                            logging.info(f"Ricevuta una non proposta (no propose==True)")

                    msg = self.subscriber.get_message(ignore_subscribe_messages=True, timeout=0.0)
                    if msg is not None:
                        self.received_messages.append(msg)
                except Exception as e:
                    logging.exception(f"Errore nella ricezione delle proposte")
                    break
            
            logging.info(f"Proposal received: {len(proposals)}, : {proposals}")
            logging.info(f"Messages received: {len(self.received_messages)}, : {self.received_messages}")

            if len(proposals)==0:        #se non ricevo proposte
                logging.info(f"No proposals for {self.active_task.task_name}")
                self.publish_task()

            # scelgo l'agente che svolge il task -------------------------------------------------------------------------------
            else:                       #se ho ricevuto proposte
                chosen_worker = self.choose_worker(proposals=proposals)
                return chosen_worker

    def choose_worker(self, proposals:list) -> str:
        '''Sceglie il worker dalla lista delle proposte e
        Inserisce il suo nome dentro il dataspace in modo che egli sappia di essere stato scelto'''
        
        choice = random.randint(0, len(proposals)-1)
        msg = proposals[choice]

        try:
            r = redis.Redis(connection_pool=self.pool)

            dict_message = json.loads(msg['data'])
            chosen_agent = dict_message['agent']
            bool_propose = dict_message['propose']
            if bool_propose:
                r.set(self.active_task.master_receiving_channel, chosen_agent, px=MAX_NETWORK_DELAY+MAX_EXECUTION_TIME+MAX_NETWORK_DELAY)       # metto il nome del primo agente della lista dentro una key con il nome del task
                logging.info(f"Chosen agent '{chosen_agent}' inserted in Redis at {self.active_task.master_receiving_channel}")
                self.chosen_worker = chosen_agent
                return chosen_agent
            else:
                logging.error("Errore: La proposta scelta non era una proposta")
                    
        except Exception as e:
            logging.exception(e)

    def update_answerer_stats(self, successful_chosen_worker):
        '''Aggiorna su redis le statistiche delle task completate dal worker che ha eseguito il task'''
        r = redis.Redis(connection_pool=self.pool)

        self.agent_set.add(successful_chosen_worker)
        if not r.get(successful_chosen_worker):
            r.set(successful_chosen_worker, 1)
        else:
            r.incr(successful_chosen_worker)

    def receive_result(self):
        '''Riceve il messaggio di risultato dell'agente scelto per il task, dopo di questo deve ripartire un nuovo task'''
        
        #ricevo il messaggio di risposta dal worker
        while True:
            msg = self.subscriber.get_message(ignore_subscribe_messages=True, timeout=(MAX_EXECUTION_TIME+MAX_NETWORK_DELAY)/1000)  #il timeout dovrebbe indicare il tempo massimo di esecuzione del task + margine

            while msg is not None:
                self.received_messages.append(msg)
                msg = self.subscriber.get_message(ignore_subscribe_messages=True, timeout=0.0)
            
            state = self.search_result()

            if state==0:
                ''' Task completato correttamente '''
                break
            elif state==-1:
                ''' Risultato non trovato nei messaggi '''

                if self.reassign_task_counter >6:
                    self.reassign_task_counter = 0
                    self.publish_task()
                    self.receive_proposes_and_assign()
            
        #Aggiorno le stat di fine task
        self.update_answerer_stats(self.chosen_worker)

    def search_result(self) -> int:
        '''Cerca il messaggio risultato tra quelli ricevuti'''
        logging.info(f"""Ricerca del messaggio di risposta in: {self.received_messages}""")
        for m in self.received_messages:
            try:
                msg_dict = json.loads(m['data'])
                if 'fail' not in msg_dict or 'result' not in msg_dict:
                    continue

                agent_answerer = msg_dict['agent']
                agent_result = msg_dict['result']
                agent_fail = msg_dict['fail']
                logging.info(f'Campi estratti da dal messaggio di risposta: agente rispondente: "{agent_answerer}", risultato: "{agent_result}", fail: "{agent_fail}"')

                if m['channel'].decode('utf-8')==self.active_task.master_receiving_channel and agent_answerer==self.chosen_worker and agent_fail==False:
                    self.active_task.result = agent_result
                    logging.info(f"Success, chosen agent {agent_answerer} has completed task {self.active_task.task_name} [{self.active_task.a, self.active_task.b}] with result {self.active_task.result}")
                    if self.only_sum and self.active_task.result != (self.active_task.a+self.active_task.b) and len(self.task_types)==1:
                        logging.error(f"!!!!!!!Risultato sbagliato!!!!!-----------------------------------------------------------------------------------------------")
                    return 0

            except Exception as e:
                logging.error(f"messaggio non formattato JSON\n{e}")
                    
        logging.error(f"Nei messaggi ricevuti non vi è il risultato cercato")
        self.received_messages = []
        self.reassign_task_counter += 1
        return -1

# scrive su txt le statistiche del master
def write_stat_agents(master:Master):
    r = redis.Redis(connection_pool = master.pool)
    f = open(f"stats.txt", "w+")     #se voglio le stat del task per ogni master, devo creare i dati nel master 
    string = ""
    for e in master.agent_set:
        string += f'{e} has accomplished {r.get(e).decode("utf-8")} tasks\n'
    f.write(string)
    f.close()

def test_connection_redis() -> None:
    try:
        r = redis.Redis(host=HOST, port=PORT, db=0)
        r.set(name='foo', value='bar', ex=1, nx=True)
        logging.info(f"Connected to Redis on '{HOST}','{PORT}'")
    except Exception as e:
        sys.exit(f"Error: No connection to Redis server on socket: ({HOST}; {PORT})\nActivate one or choose an available socket")


if __name__ == "__main__":
    test_connection_redis()

    master = Master(only_sum=True)
    logging.info("Start pushing tasks")
    while True:

        master.create_task()
        #pubblico task-----------------------------------------------------------------------------------------------------------
        master.publish_task()
        #ricevo le proposte degli slave (canale)---------------------------------------------------------------------------------
        chosen_worker = master.receive_proposes_and_assign()
        
        master.receive_result()

        write_stat_agents(master)
        
        time.sleep(REPUBLISH_DELAY)