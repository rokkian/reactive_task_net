import redis.clients.jedis.*;
//import org.json.*;

/* Questo deve essere il primo "Agente" che risponde a un canale di stimoli
 * 
 * */

public class JedisClassAuth {
	static final String redisHost = "192.168.1.17";//"localhost";
	static final Integer redisPort = 6379;
	static final String IdAgente = "Agent"+String.valueOf((int)(Math.random()*10000)); 		//va scelto un identificativo del singolo agente slave
	static final boolean VERBOSE = true;
	
	public static void main(String[] args) {
		final JedisPool pool = new JedisPool(redisHost, redisPort);
		Jedis jSub = pool.getResource();
		System.out.println("Start Slave '"+IdAgente+"' !!!");
		//final ArrayList<String> taskSet = new ArrayList<String>();
		//final ArrayList<Thread> threads = new ArrayList<Thread>();
		
		//psubscribe a un pattern di canali
		jSub.psubscribe(new JedisPubSub() {
			@Override
	        public void onPMessage(final String pattern, final String channel, final String message) {
				
				ThreadMessageHandler t = new ThreadMessageHandler(IdAgente, pool, pattern, channel, message);
				t.start();
				
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
