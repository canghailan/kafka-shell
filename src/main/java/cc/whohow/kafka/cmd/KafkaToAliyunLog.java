package cc.whohow.kafka.cmd;

import cc.whohow.kafka.util.CSV;
import cc.whohow.kafka.util.Props;
import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.log.common.LogItem;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class KafkaToAliyunLog implements Runnable {
    private static final Logger log = LogManager.getLogger(KafkaToAliyunLog.class);
    private final Properties properties;

    public KafkaToAliyunLog(Properties properties) {
        this.properties = properties;
    }

    public void run() {
        Properties sourceProps = Props.sub(properties, "source.");
        long pollTimeout = Long.parseLong(sourceProps.getProperty("poll.timeout.ms", "5000"));
        Collection<String> topic = CSV.parse(sourceProps.getProperty("topic", ""));
        String sinkProject = properties.getProperty("sink.project");
        String sinkEndpoint = properties.getProperty("sink.endpoint");
        String sinkAccessKeyId = properties.getProperty("sink.accessKeyId");
        String sinkAccessKeySecret = properties.getProperty("sink.accessKeySecret");
        String sinkLogStore = properties.getProperty("sink.logStore");

        Producer producer = new LogProducer(new ProducerConfig());
        producer.putProjectConfig(new ProjectConfig(sinkProject, sinkEndpoint, sinkAccessKeyId, sinkAccessKeySecret));
        try (Consumer<String, String> consumer = new KafkaConsumer<>(sourceProps, new StringDeserializer(), new StringDeserializer())) {
            if (topic.isEmpty()) {
                consumer.subscribe(Pattern.compile(".*"), new NoOpConsumerRebalanceListener());
            } else {
                consumer.subscribe(topic);
            }
            while (true) {
                try {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
                    if (consumerRecords.isEmpty()) {
                        continue;
                    }
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        LogItem logItem = new LogItem(getLogTime(record));
                        logItem.PushBack("topic", record.topic());
                        logItem.PushBack("partition", Long.toString(record.partition()));
                        logItem.PushBack("offset", Long.toString(record.offset()));
                        logItem.PushBack("key", record.key());
                        logItem.PushBack("value", record.value());
                        producer.send(sinkProject, sinkLogStore, record.topic(), null, logItem);
                        log.debug("{}({}, {}) -> {} {}",
                                record.topic(), record.partition(), record.offset(),
                                sinkProject, sinkLogStore);
                    }
                    consumer.commitAsync();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        } finally {
            try {
                producer.close();
            } catch (Exception ignore) {
            }
        }
    }

    private int getLogTime(ConsumerRecord<?, ?> record) {
        if (record.timestamp() > 0) {
            return (int) TimeUnit.MILLISECONDS.toSeconds(record.timestamp());
        } else {
            return (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        }
    }
}
