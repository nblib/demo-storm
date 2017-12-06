package demo.hewe.trident;

import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * @year 2017
 * @project demo-storm
 * @description:<p></p>
 **/
public class TridentMain {
    public static void main(String[] args) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("nihao"),
                new Values("hello"));
        spout.setCycle(true);
    }
}
