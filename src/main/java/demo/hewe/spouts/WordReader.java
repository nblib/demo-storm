package demo.hewe.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * @year 2017
 * @project demo-storm
 * @description:<p></p>
 **/
public class WordReader implements IRichSpout {
    private boolean completed = false;
    Reader fileReader;
    private SpoutOutputCollector collector;
    private TopologyContext context;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.context = context;
            this.fileReader = new FileReader(conf.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file[" + conf.get("wordFile") + "]");
        }
        this.collector = collector;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if (completed) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                //DO nothing
            }
            return;
        }
        String str;

        BufferedReader reader = new BufferedReader(fileReader);

        try {
            while ((str = reader.readLine()) != null) {
                this.collector.emit(new Values(str), str);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error read file", e);
        } finally {
            completed = true;
        }

    }

    public void ack(Object msgId) {
        System.out.println("OK: " + msgId);
    }

    public void fail(Object msgId) {
        System.out.println("FAIL: " + msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
