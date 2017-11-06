package ar.edu.itba.pod.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {

        //TODO desharcodear
        ArrayList<String> ad = new ArrayList<>();
        ad.add("127.0.0.1");


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
                            .addInterface("10.17.*.*"))
                        )
                .setGroupConfig(new GroupConfig()
                        .setName("56382-54308-55291-53559")
                        .setPassword("56382-54308-55291-53559"))

                .setInstanceName("56382-54308-55291-53559")
                .addMapConfig(new MapConfig().setName("56382-54308-55291-53559-map1"))
                .addMapConfig(new MapConfig().setName("56382-54308-55291-53559-map2"))
                .addMapConfig(new MapConfig().setName("56382-54308-55291-53559-map3"))
                .addMapConfig(new MapConfig().setName("56382-54308-55291-53559-map4"))
                .addMapConfig(new MapConfig().setName("56382-54308-55291-53559-map5"))
                .addMapConfig(new MapConfig().setName("56382-54308-55291-53559-map6"))
                .addMapConfig(new MapConfig().setName("56382-54308-55291-53559-map7"));

        // Set minimum cluster size
        //config.setProperty("hazelcast.initial.min.cluster.size","2");
        //config.setProperty("hazelcast.network.join.multicast.enabled", "false");

       Hazelcast.newHazelcastInstance(config);


    }
}
