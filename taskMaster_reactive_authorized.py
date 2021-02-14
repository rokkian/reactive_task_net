from json.decoder import JSONDecodeError
import redis
import logging
import random
import time
import json
import sys
from typing import Final

from redis.client import PubSub, Redis                # ~const non proprio const, serve Python 3.8

"""
Questo è il master sincrono statico base, il suo compito è:
    1) generare un task e notificarlo agli iscritti
    2) leggere le proposte degli iscritti disponibili
    3) scegliere l'iscritto che svolga il task
    4) leggere la notifica di completamento del task e recuperare il risultato
    5) Ripetere
"""

HOST : Final = "localhost"
PORT : Final = 6379
MASTER_NAME : Final = f"master{random.randint(0, 1e4)}_"

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

pool = redis.ConnectionPool(host=HOST, port=PORT)
taskTypes = ["task+", "task-", "task*", "task/"]
only_sum = False
#taskTypes = ["task+"]
task_counter = 0
agent_set = set()

class Task:
    def __init__(self):
        self.task_type = taskTypes[random.randint(0,len(taskTypes)-1)]
        self.task_name = f"{self.task_type}_{task_counter}"
        self.a = random.randint(1, 100)
        self.b = random.randint(1, 100)
        self.result = None
        self.master_receiving_channel = MASTER_NAME+self.task_name    # lo imposto come identificativo del task

    def __repr__(self) -> str:
        return self.master_receiving_channel

def write_stat_agents():
    r = redis.Redis(connection_pool=pool)
    f = open(f"masters_stats_auth/stats.txt", "w+")     #se voglio le stat die task per ogni master, devo creare i dati nel master 
    string = ""
    for e in agent_set:
        string += f'{e} has accomplished {r.get(e).decode("utf-8")} tasks\n'
    f.write(string)
    f.close()

def publish_task(redis, task:Task ) -> None:
    task_message_json = json.dumps(task.__dict__)
    n = r.publish(task.task_name, task_message_json)    #invio la key dove inserire il risultato e i numeri
    logging.info(f'\nTask published "{task_message_json}" received by {n} agents')

    while n == 0:
        time.sleep(0.1)
        n = r.publish(task.task_name, task_message_json)    #invio la key dove inserire il risultato e i numeri
        logging.info(f'Task published "{task_message_json}" received by {n} agents')

def receive_proposes(r:Redis, p:PubSub, active_task: Task):
    #ricevo le proposte degli slave (canale)---------------------------------------------------------------------------------
        messages = []
        counter = 0
        while True:
            time.sleep(0.05)                                                       # attende messaggi dagli agenti (prova a rimuovere)
            msg = p.get_message(ignore_subscribe_messages=True, timeout=0.0)
            
            while msg is not None:
                try:
                    if msg["channel"].decode('utf-8') == active_task.master_receiving_channel and msg['data'].decode("utf-8")[0]=="{" and json.loads(msg['data'])['propose']:
                        messages.append(msg)
                    msg = p.get_message(ignore_subscribe_messages=True, timeout=0.0)
                except Exception as e:
                    break
            
            logging.info(f"Proposal received: {len(messages)}")

            # scelgo l'agente che svolge il task (casuale)-------------------------------------------------------------------------------

            if len(messages)==0:        #se non ricevo messaggi
                logging.info(f"No proposals for {active_task.task_name}")
                counter += 1
                if counter>6:
                    logging.info(f"Proposal time staled: re-publishing task")
                    publish_task(r, active_task)
                    counter = 0
                
            else:                       #se ricevo messaggi
                choice = random.randint(0, len(messages)-1)
                msg = messages[choice]               # forma messaggi: {'agent':'Agent001', 'propose':true}
                print(f"Agente scelto per il task: ", msg)
                try:
                    dict_message = json.loads(msg['data'])
                    chosen_agent = dict_message['agent']
                    bool_propose = dict_message['propose']
                    if bool_propose:
                        r.set(active_task.master_receiving_channel, chosen_agent, ex=15)                                                # metto il nome del primo agente della lista dentro una key con il nome del task
                        logging.info(f"Chosen agent '{chosen_agent}' inserted in Redis at {active_task}")
                        return chosen_agent
                        break
                    
                except Exception as e:
                    logging.exception(e)

if __name__ == "__main__":
    try:
        r = redis.Redis(connection_pool=pool) 
        p = r.pubsub()
        p.subscribe("to-master")
    except Exception:
        sys.exit(f"Error: No connection to Redis server on socket: ({HOST}; {PORT})\nActivate one or choose an available socket")

    logging.info("Start pushing tasks")
    while True:        
        active_task = Task()

        #pubblico task-----------------------------------------------------------------------------------------------------------
        p.subscribe(active_task.master_receiving_channel)
        logging.info(f"Subscription to {active_task.master_receiving_channel}")
        publish_task(r, active_task)        

        #ricevo le proposte degli slave (canale)---------------------------------------------------------------------------------
        chosen_agent = receive_proposes(r=r, p=p, active_task=active_task)
        
        #ricevo il risultato (eventualmente ricevo un fail e riassegno il calcolo)-----------------------------------------------------------------------
        counter = 0
        while True:
            msg = p.get_message(ignore_subscribe_messages=True, timeout=1.0)        #allo scadere del timeout posso provare a riproporre il task
            if counter>5:
                counter = 0
                print("counter maggiore di 5, possibile stale risposta")
                #publish_task(r, active_task)        #va rimodulato il programma con l'aggiunta della riassegnazione del task
                chosen_agent = receive_proposes(r=r, p=p, active_task=active_task)
                print("terzo stato-----------------------------------------------")
                continue
            
            if msg is None:
                counter += 1
                continue

            try:
                message_corp = json.loads(msg['data'])
                agent_answerer = message_corp['agent']
                active_task.result = message_corp['result']
                task_name_answer = message_corp['task_name']

                if chosen_agent==agent_answerer and task_name_answer==active_task.task_name:
                    logging.info(f"Success, chosen agent {chosen_agent} has completed task {active_task.task_name} [{active_task.a,active_task.b}] with result {active_task.result}")
                    if only_sum and active_task.result != (active_task.a+active_task.b) and len(taskTypes)==1:
                        logging.error(f"!!!!!!!Risultato sbagliato!!!!!-----------------------------------------------------------------------------------------------")
                    break

            except Exception as e:
                counter +=1

                logging.error(e)
                logging.error(msg)
                continue

            #controllo che il messaggio ricevuto sia quello che cerco
            
        p.unsubscribe(active_task.master_receiving_channel)
        logging.info(f"Unsubscription to {active_task.master_receiving_channel}")

        agent_set.add(agent_answerer)
        if not r.get(agent_answerer):
            r.set(agent_answerer, 1)
        else:
            r.incr(agent_answerer)

        write_stat_agents()

        task_counter += 1
        #time.sleep(0)