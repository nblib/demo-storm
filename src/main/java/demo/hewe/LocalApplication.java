package demo.hewe;

import demo.hewe.kafka.KafkaConsumerTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

/**
 * @year 2017
 * @project demo-storm
 * @description:<p></p>
 **/
public class LocalApplication {
    public static void main(String[] args) {
        Config config = new Config();
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        //本地运行模式
        LocalCluster localCluster = new LocalCluster();
        StormTopology topology = KafkaConsumerTopology.newTopology(" 10.169.0.214:9092", "logstash_test");
        localCluster.submitTopology("Getting-Started-Toplogie", config, topology);
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        localCluster.shutdown();
    }

}
