package demo.hewe.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Map;

/**
 * @year 2017
 * @project demo-storm
 * @description:<p></p>
 **/
public class WordNormalizer implements IRichBolt {
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String string = input.getString(0);
        String[] split = string.split(" ");
        for (String word : split) {
            word = word.trim();
            if (!word.isEmpty()) {
                ArrayList<Tuple> tuples = new ArrayList<Tuple>();
                tuples.add(input);
                collector.emit(tuples, new Values(word));
            }
        }
        collector.ack(input);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
