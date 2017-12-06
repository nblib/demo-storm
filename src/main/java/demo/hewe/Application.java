package demo.hewe;

import demo.hewe.bolts.WordCounter;
import demo.hewe.bolts.WordNormalizer;
import demo.hewe.spouts.WordReader;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Hello world!
 */
public class Application {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("word-reader", new WordReader());
        topologyBuilder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        topologyBuilder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));

        Config config = new Config();
        config.put("wordsFile", args[0]);
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        //本地运行模式
       /* LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("Getting-Started-Toplogie", config, topologyBuilder.createTopology());
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        localCluster.shutdown();*/
        //集群运行模式
        try {
            StormSubmitter.submitTopology("Count-Word-Topology-With-Refresh-Cache", config,
                    topologyBuilder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }
}
