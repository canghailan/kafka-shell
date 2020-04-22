package cc.whohow.kafka.cmd;

import cc.whohow.kafka.util.CSV;
import cc.whohow.kafka.util.IO;
import cc.whohow.kafka.util.Props;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class StdinToKafka implements Runnable {
    private final Properties properties;

    public StdinToKafka(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void run() {
        Properties sinkProps = Props.sub(properties, "sink.");
        Collection<String> topics = CSV.parse(sinkProps.getProperty("topic"));
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("sink.topic: " + topics);
        }

        try (Producer<String, String> producer = new KafkaProducer<>(sinkProps, new StringSerializer(), new StringSerializer())) {
            while (true) {
                // key:
                // value: EOF
                // {"test":true}
                // EOF
                String key = expect("key", IO::readLine);
                String value = expect("value", IO::readLines);
                for (String topic : topics) {
                    RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key.isEmpty() ? null : key, value)).get();
                    System.out.println(format(metadata));
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

    private String expect(String name, Supplier<String> supplier) {
        System.out.print(name + ": ");
        return supplier.get();
    }

    private String format(RecordMetadata metadata) {
        return metadata.topic() + "(" + metadata.partition() + ", " + metadata.offset() + ")\t" + metadata.checksum();
    }
}
