package demo.hewe.kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @year 2017
 * @project demo-storm
 * @description:<p></p>
 **/
public class StringBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Logger log = LoggerFactory.getLogger(StringBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //获取的tupe中包含如下Field(键值对):
        // "topic" -> "logstash_test"
        //"partition" -> "0"
        //"offset" -> "1736"
        // "key" -> "null"
        // "value" -> "asad"
        String string = (String) input.getValueByField("value");
        log.info("get data: {}", string);
        this.collector.emit(input, new Values(string));
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
