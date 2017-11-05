package ar.edu.itba.pod.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {

        //TODO desharcodear
        ArrayList<String> ad = new ArrayList<>();
        ad.add("10.2.71.28");
        ad.add("10.2.69.200");


        logger.info("Hazelcast Server Starting ...");
        Config config = new Config()
                .setNetworkConfig(new NetworkConfig()
                .setJoin(new JoinConfig()
                    .setMulticastConfig(new MulticastConfig()
                        .setEnabled(false))
                    .setTcpIpConfig(new TcpIpConfig()
                        .setEnabled(true)
                        .setMembers(ad)))
                    .setInterfaces(new InterfacesConfig()
                        .setEnabled(true)
                        //TODO desharcodear
                        .addInterface("10.2.*.*"))
                        )

                .setInstanceName("TPE-Cluster")
                .addMapConfig(new MapConfig().setName("people"))
                .addMapConfig(new MapConfig().setName("departments"))
                .addMultiMapConfig(new MultiMapConfig().setName("people"));


        // Set minimum cluster size
        //config.setProperty("hazelcast.initial.min.cluster.size","2");
        //config.setProperty("hazelcast.network.join.multicast.enabled", "false");

        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);


    }
}
