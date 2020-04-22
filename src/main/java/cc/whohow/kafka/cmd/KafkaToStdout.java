package cc.whohow.kafka.cmd;

import cc.whohow.kafka.util.CSV;
import cc.whohow.kafka.util.Props;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaToStdout implements Runnable {
    private final Properties properties;

    public KafkaToStdout(Properties properties) {
        this.properties = properties;
    }

    public void run() {
        Properties sourceProps = Props.sub(properties, "source.");
        long pollTimeout = Long.parseLong(sourceProps.getProperty("poll.timeout.ms", "5000"));
        Collection<String> topic = CSV.parse(sourceProps.getProperty("topic", ""));

        try (Consumer<String, String> consumer = new KafkaConsumer<>(sourceProps, new StringDeserializer(), new StringDeserializer())) {
            if (topic.isEmpty()) {
                consumer.subscribe(Pattern.compile(".*"), new NoOpConsumerRebalanceListener());
            } else {
                consumer.subscribe(topic);
            }
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println(format(record));
                }
                consumer.commitAsync();
            }
        }
    }

    private String format(ConsumerRecord<?, ?> record) {
        return record.topic() + "(" + record.partition() + ", " + record.offset() + ")\t" + record.key() + "\t" + record.value();
    }
}
