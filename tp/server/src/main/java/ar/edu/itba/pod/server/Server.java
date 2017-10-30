package ar.edu.itba.pod.server;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {

        logger.info("hazelcast Server Starting ...");
        Config config = new Config();
        config.setInstanceName("TPE-Cluster");
        config.addMapConfig(new MapConfig().setName("people"));

        // Set minimum cluster size
        //config.setProperty("hazelcast.initial.min.cluster.size","2");
        //config.setProperty("hazelcast.network.join.multicast.enabled", "false");

        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);


    }
}
