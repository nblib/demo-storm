package demo.hewe.kafka;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @year 2017
 * @project demo-storm
 * @description:<p>kafka消费拓扑,从kafka中获取数据使用stringbolt接收,打印到日志中</p>
 **/
public class KafkaConsumerTopology {
    private static final String SPOUT_ID = "demo-storm-kafka_consumer";
    private static final String STRING_BOLT_ID = "jsonProject-bolt";

    public static StormTopology newTopology(String addr, String topic) {
        final TopologyBuilder builder = new TopologyBuilder();
        //自带的kafka消费者配置信息
        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder("10.169.0.214:9092", "logstash_test")
                .setGroupId("demo_test").setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST).build();
        //使用storm-kafka-client包中的KafkaSpout
        builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConfig));
        //使用自定义的Bolt
        builder.setBolt(STRING_BOLT_ID, new StringBolt()).shuffleGrouping(SPOUT_ID);
        return builder.createTopology();
    }
}
