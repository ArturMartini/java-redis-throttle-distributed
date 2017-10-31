package throttle;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@RestController
public class ThrottleController {

	@Autowired
	public Jedis jedis;

	private final String counterPool = "ssb:pool";
	private final String counterMax = "ssb:pool:max";
	private final String counterCount = "ssb:pool:count";
	private final String counterSec = "ssb:pool:sec";

	@RequestMapping(method = RequestMethod.GET, value = "/redisIncr")
	public void validateRedisIncrementIsAtomic() {
		ExecutorService ex = Executors.newFixedThreadPool(10);
		
		for(int i = 0; i < 10; i++){
			ex.execute(new Runnable() {
				public void run() {
					for(int n = 0; n < 400; n++){
						
						JedisPoolConfig config = new JedisPoolConfig();
						config.setMaxTotal(50);
						
						JedisPool pool = new JedisPool(config, "localhost");
						Jedis jedis = pool.getResource();
						
						long num = jedis.incr(counterPool);
						jedis.getClient().close();
						System.out.println(Thread.currentThread().getName() + " | " + num);
					}
				}
			});
		}
	}

	
	@RequestMapping(method = RequestMethod.GET, value = "/callThrottleService")
	public ResponseEntity<String> callThrottleService() {

		boolean checkRedis = true;
		
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(50);
    
		JedisPool jedisPool = new JedisPool(config, "localhost");
		Jedis jedis = jedisPool.getResource();
		
		if (jedis.get(counterPool) == null) {
			jedis.set(counterPool, "0");
		}

		if (jedis.get(counterCount) == null) {
			jedis.set(counterCount, "0");
		}

		if (jedis.get(counterMax) == null) {
			jedis.set(counterMax, "5");
		}
		
		if (jedis.get(counterSec) == null) {
			jedis.set(counterSec, "0");
		}

		if (checkRedis) {
			while (true) {
				Integer limit = Integer.parseInt(jedis.get(counterMax));
				long pool = jedis.incr(counterPool);

				if (pool <= limit) {
					if(jedis.ttl(counterPool) < 0)
						jedis.expire(counterPool, Integer.valueOf(jedis.get(counterSec)));
					
					callService();
				} else {
					System.out.println("too many requests per second | " + jedis.ttl(counterPool));
					
					try {
						Thread.sleep(10);
						continue;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				jedis.getClient().close();
			}
		}
		return new ResponseEntity<String>(HttpStatus.OK);
	}

	public void callService() {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:SSSSSS");
			System.out.println(Thread.currentThread().getName() + " | Call Service | " + sdf.format(new Date()));
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
