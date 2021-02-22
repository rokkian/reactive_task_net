import java.util.HashSet;
import java.util.Set;
import redis.clients.jedis.*;

/* Questo deve essere il primo "Agente" che risponde a un canale di stimoli
 * 
 * */

public class JedisClassAuth {
	//static final String redisHost = "localhost"; 
	static final String redisHost = "192.168.1.17";
	static final Integer redisPort = 6379;
	static final long MAX_EXECUTION_TIME = 3000;	//ms
	static final long MAX_NETWORK_DELAY = 150;		//ms
	
	static final String IdAgente = "Agent"+String.valueOf((int)(Math.random()*10000)); 		//va scelto un identificativo del singolo agente slave
	static final boolean VERBOSE = true;
	static final Set<String> task_history = new HashSet<String>();
	
	public static boolean isConnected(JedisPool pool) {
		try {
			Jedis r = pool.getResource();
			r.setex("foo", 1, "bar");
			return true;
		} catch (Exception e) {
			return false;
		}
		
	}
	
	public static void main(String[] args) {
		
		final JedisPool pool;
		Jedis jSub;
		try {
			pool = new JedisPool(redisHost, redisPort);
			if (!isConnected(pool)) throw new Exception("Redis a ('"+redisHost+"', '"+redisPort+"') non connesso");
			jSub = pool.getResource();
		} catch (Exception e) {
			System.err.println("Redis not connected");
			e.printStackTrace();
			return;
		}
		System.out.println("Start Slave '"+IdAgente+"' !!!");
		System.out.println("Connected to Redis on ('"+redisHost+"', "+redisPort+")");
		//final ArrayList<String> taskSet = new ArrayList<String>();
		//final ArrayList<Thread> threads = new ArrayList<Thread>();
		
		//psubscribe a un pattern di canali
		jSub.psubscribe(new JedisPubSub() {
			@Override
	        public void onPMessage(final String pattern, final String channel, final String message) {
				//System.out.println("Ci sono "+Thread.activeCount()+" Thread attivi.");
				if (!task_history.contains(message)){
					task_history.add(message);
					ThreadMessageHandler t = new ThreadMessageHandler(IdAgente, pool, pattern, channel, message, MAX_EXECUTION_TIME, MAX_NETWORK_DELAY);
					t.start();
					
				}
					

	        }
			
			@Override
	        public void onPSubscribe(final String pattern, final int subscribedChannels) {
	            //new ThreadSubscribeHandler(IdAgente, pool, pattern, subscribedChannels).start();
	        }
			
		}, "tas?*");	//nome canale da ascoltare
		
		jSub.close();
		pool.close();
	}
}