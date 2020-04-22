package cc.whohow.kafka.cmd;

import cc.whohow.kafka.util.CSV;
import cc.whohow.kafka.util.Props;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Pattern;

public class KafkaToKafka implements Runnable {
    private static final Logger log = LogManager.getLogger(KafkaToKafka.class);
    private final Properties properties;

    public KafkaToKafka(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void run() {
        Properties sourceProps = Props.sub(properties, "source.");
        Properties sinkProps = Props.sub(properties, "sink.");
        long pollTimeout = Long.parseLong(sourceProps.getProperty("poll.timeout.ms", "5000"));
        List<String> sourceTopic = CSV.parse(sourceProps.getProperty("topic", ""));
        List<String> sinkTopic = CSV.parse(sinkProps.getProperty("topic", ""));
        if (sourceTopic.size() != sinkTopic.size()) {
            throw new IllegalArgumentException();
        }
        Map<String, String> topicMapping = new HashMap<>();
        for (int i = 0; i < sinkTopic.size(); i++) {
            topicMapping.put(sourceTopic.get(i), sinkTopic.get(i));
        }
        Function<String, String> mapTopic = topicMapping.isEmpty() ? Function.identity() : topicMapping::get;

        try (Producer<ByteBuffer, ByteBuffer> producer = new KafkaProducer<>(sinkProps, new ByteBufferSerializer(), new ByteBufferSerializer());
             Consumer<ByteBuffer, ByteBuffer> consumer = new KafkaConsumer<>(sourceProps, new ByteBufferDeserializer(), new ByteBufferDeserializer())) {
            if (sourceTopic.isEmpty()) {
                consumer.subscribe(Pattern.compile(".*"), new NoOpConsumerRebalanceListener());
            } else {
                consumer.subscribe(sourceTopic);
            }
            while (true) {
                try {
                    ConsumerRecords<ByteBuffer, ByteBuffer> consumerRecords = consumer.poll(pollTimeout);
                    if (consumerRecords.isEmpty()) {
                        continue;
                    }
                    for (ConsumerRecord<ByteBuffer, ByteBuffer> record : consumerRecords) {
                        try {
                            RecordMetadata recordMetadata = producer.send(
                                    new ProducerRecord<>(mapTopic.apply(record.topic()), record.key(), record.value())).get();
                            log.debug("{}({}, {}) -> {}({}, {})",
                                    record.topic(), record.partition(), record.offset(),
                                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                    consumer.commitAsync();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
}
