package demo.hewe.kafka;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.topology.TopologyBuilder;
import java.util.Properties;

/**
 * @year 2017
 * @project demo-storm
 * @description:<p>kafka生产拓扑,用于生产数据到kafka</p>
 **/
public class KafkaProducerTopology {
    /**
     * @param brokerUrl Kafka broker URL
     * @param topicName Topic to which publish sentences
     * @return A Storm topology that produces random sentences using {@link RandomSentenceSpout} and uses a {@link KafkaBolt} to
     * publish the sentences to the kafka topic specified
     */
    public static StormTopology newTopology(String brokerUrl, String topicName) {
        final TopologyBuilder builder = new TopologyBuilder();
        //使用自带的一个随机生产消息的Spout,用于测试.
        builder.setSpout("spout", new RandomSentenceSpout.TimeStamped(""), 2);

        /* The output field of the RandomSentenceSpout ("word") is provided as the boltMessageField
          so that this gets written out as the message in the kafka topic. */
        final KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(newProps(brokerUrl, topicName))
                .withTopicSelector(new DefaultTopicSelector(topicName))
                .<String, String>withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>("key", "word"));

        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");

        return builder.createTopology();
    }

    /**
     * @return the Storm config for the topology that publishes sentences to kafka using a kafka bolt.
     */
    private static Properties newProps(final String brokerUrl, final String topicName) {
        return new Properties() {{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.CLIENT_ID_CONFIG, topicName);
        }};
    }
}
