import redis.clients.jedis.*;

public class ThreadSubscribeHandler extends Thread{
	//definisci i campi
	JedisPool pool;
	String IdAgent;
	String pattern;
	int subscribedChannels;
	boolean terminate=false;
	
	public ThreadSubscribeHandler(String IdAgent, JedisPool pool, String pattern, int subscribedChannels) {
		super();
		this.IdAgent = IdAgent;
		this.pattern = pattern;
		this.pool = pool;
		this.subscribedChannels = subscribedChannels;
	}
	
	@Override
	public void run() {
		try {
            System.out.println("Iscritto a "+this.subscribedChannels+" canali");
            Jedis j = this.pool.getResource();
            j.publish("task-notify", IdAgent);
            j.close();
		} catch (Exception e) {
			System.err.println(e);
		}
		
	}
}
