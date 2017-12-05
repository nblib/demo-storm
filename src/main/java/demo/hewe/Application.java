package demo.hewe;

import demo.hewe.bolts.WordCounter;
import demo.hewe.bolts.WordNormalizer;
import demo.hewe.spouts.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
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
        config.put("wordsFile", "D:/words.txt");
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("Getting-Started-Toplogie", config, topologyBuilder.createTopology());
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        localCluster.shutdown();
    }
}
