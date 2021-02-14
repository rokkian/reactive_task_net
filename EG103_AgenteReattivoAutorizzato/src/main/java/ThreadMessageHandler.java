import redis.clients.jedis.*;
import org.json.*;
//import java.util.Random;


// DA MODIFICARE
/* Definisco il thread che deve gestire una sessione per l'agente:
 * voglio che:
 * 		1) Risponda a un messaggio dicendo che è disponibile a svolgere il task proposto
 * 		2) Capisca di essere stato scelto come esecutore 
 * 		3) Esegue il task, oppure esce
 * 		4) Invia al master il risultato
 * */

public class ThreadMessageHandler extends Thread{
	//definisci i campi	
	JedisPool pool;
	String IdAgent;
	String pattern;
	String taskName;	
	String taskType;
	String masterTaskName;	//è il canale su cui bisogna rispondere al master
	String message;
	String channel;
	String hashKey;
	String responseJSON;
	JSONObject messageJSON;
	double a, b, result;
	boolean terminate;
	boolean acceptTask;
	boolean authorized=false;
	
	private void getResult() {
		if (taskType.equalsIgnoreCase("task+")) {
			result = a+b;
		} else if (taskType.equalsIgnoreCase("task-")) {
			result = a-b;
		} else if (taskType.equalsIgnoreCase("task*")) {
			result = a*b;
		} else if (taskType.equalsIgnoreCase("task/")) {
			result = a/b;
		} else {
			System.out.println("Operazione non riconosciuta, invio messaggio fallimento");
			Jedis tmp_j = pool.getResource();
			String responseJSON = "{\"result\":"+null+","
					+ "\"agent\":\""+IdAgent+"\","
					+ "\"fail\":true ,"
					+ "\"task_name\":\""+taskName+"\""
					+ "}";
			tmp_j.publish(this.masterTaskName, responseJSON);
			tmp_j.close();
		}
	}
	private boolean propose() {
		// Risposta al messaggio del master con accettazione compito
		try {
			Jedis j = this.pool.getResource();
			String acceptMessage = "{\"agent\":\""+this.IdAgent+"\", "
									+"\"propose\":true"
									+"}";
			j.publish(this.masterTaskName, acceptMessage);
			System.out.println("Proposto per task: "+this.masterTaskName+"; con: "+acceptMessage);
			j.close();
			return true;
		} catch (Exception e) {
			System.err.println("Propose message hasn't been send");
			return false;
		}
	}
	
	
	private void execute() {
		System.out.printf("Task Autorizzato %s, agente %s\n", taskName, IdAgent);
		
		getResult();
		
		//mando al master il risultato come testo messaggio in JSON
		this.responseJSON = "{\"result\":"+String.valueOf(this.result)+","
				+ "\"agent\":\""+IdAgent+"\","
				+ "\"task_name\":\""+taskName+"\","
				+ "\"master_receiving_channel\":\""+this.masterTaskName+"\","
				+ "\"fail\":false"
				+ "}";
	}
	
	public ThreadMessageHandler(String IdAgent, JedisPool pool, String pattern, String channel, String message) {
		super();
		this.terminate=false;
		this.acceptTask=true;			//si può fare casuale
		this.IdAgent = IdAgent;				// Id agente di calcolo
		this.pool = pool;
		this.pattern = pattern;
		this.channel = channel;
		
		try {
			this.messageJSON = new JSONObject(message);
			this.taskName = this.messageJSON.getString("task_name");				//è il nome del task, Eg: task+_5
			this.a = this.messageJSON.getDouble("a");
			this.b = this.messageJSON.getDouble("b");
			this.masterTaskName = messageJSON.getString("master_receiving_channel");
			this.taskType = messageJSON.getString("task_type");
		} catch (Exception e) {
			System.out.println("Messaggio non serializzato JSON o non completo");
			terminate=true;
		}
		
	}
	
	@Override
	public void run() {		
		try {
			System.out.println("\nDato pattern '"+this.pattern+"', da canale '"+this.channel+"', messaggio: '"+this.masterTaskName+"'.");
			System.out.println("Inizio del Thread "+Thread.currentThread().getName());
			if (terminate) {System.out.println("Chiuso il Thread: "+Thread.currentThread().getName());return;}
			
			//propongo il thread per il compito al master
			boolean prop= this.propose();
			
			if (prop) {
				
				// Ricezione conferma o return del Thread-------------------------------------------------------------
				// conferma come lettura d una key 'nome_task' e il nome dell'agente corrisponde di ha l'auth oppure si disfa il thread
				Jedis j = pool.getResource();
				String authorizedAgent = j.get(this.masterTaskName);
				int counter = 0;
				while (true) {
					if (authorizedAgent == null || authorizedAgent.isBlank()) {	//key non ancora creata
						System.out.println("Leggo "+authorizedAgent+" su: "+this.masterTaskName);
						Thread.sleep(50);
						authorizedAgent = j.get(this.masterTaskName);
						counter++;
						if (counter > 12) {System.out.println("Master took too long to insert "+this.masterTaskName+" executioner"); break;}
					} else if (authorizedAgent.equalsIgnoreCase(IdAgent)) {	//agente autorizzato
					
						// Esecuzione task-------------------------------------------------------------------------------
						System.out.println("Scelto con "+IdAgent+"=="+authorizedAgent+" su: "+taskName);
						//Thread.sleep(1000);
						execute();
					
						long n = j.publish(this.masterTaskName, this.responseJSON);
						if (n==0) {System.err.println("Nessuno ha ricevuto la risposta!");}
						System.out.printf("\nTask '%s' Success (response received by %d agents); Chiudo il thread: '%s'\n",taskName, n,Thread.currentThread().getName());					
						break;
					} else if (authorizedAgent != null){	//agente non autorizzato
						System.out.println("Non scelto "+IdAgent+"!="+authorizedAgent+" per: "+taskName);
						break;
					}
				
				}
				j.close();
			}		
			
		} catch (Exception e) {
			System.err.println("Generic error in message handler\n"+e);
			e.printStackTrace();
		}
		
	}
}
