package cc.whohow.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class KafkaShell {
    private static final String TOPIC = "topic";
    private static final long TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {
        while (true) {
            String mode = read("producer or consumer?(p/c)");
            if ("p".equalsIgnoreCase(mode) || "producer".equalsIgnoreCase(mode)) {
                produce();
                return;
            }
            if ("c".equalsIgnoreCase(mode) || "consumer".equalsIgnoreCase(mode)) {
                consume();
                return;
            }
        }
    }

    private static void produce() throws Exception {
        Properties properties = readProduceProperties();
        List<String> topics = list(read(TOPIC));

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        try {
            String key = read("key");
            String value = readLines("value");
            for (String topic : topics) {
                RecordMetadata metadata = producer.send(new ProducerRecord<String, String>(topic, key, value)).get();
                System.out.println(format(metadata));
            }
        } finally {
            producer.close();
        }
    }

    private static Properties readProduceProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, read(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, read(ProducerConfig.CLIENT_ID_CONFIG));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static void consume() throws Exception {
        Properties properties = readConsumerProperties();
        List<String> topics = list(read(TOPIC));

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(TIMEOUT);
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println(format(record));
                }
                consumer.commitAsync();
            }
        } finally {
            consumer.close();
        }
    }

    private static Properties readConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, read(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, read(ConsumerConfig.GROUP_ID_CONFIG));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, read(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    private static String read(String name) {
        System.out.print(name + ": ");
        return read();
    }

    private static String read() {
        return new Scanner(System.in).nextLine().trim();
    }

    private static String readLines(String name) {
        System.out.print(name + ":\n");
        return readLines();
    }

    private static String readLines() {
        Scanner scanner = new Scanner(System.in);
        scanner.useDelimiter(scanner.nextLine());
        return scanner.next().replaceAll("\r?\n?$", "");
    }

    private static List<String> list(String text) {
        if (text == null || text.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> list = new ArrayList<String>();
        for (String string : text.split(",")) {
            String value = string.trim();
            if (value.isEmpty()) {
                continue;
            }
            list.add(value);
        }
        return list;
    }

    private static String format(RecordMetadata metadata) {
        return metadata.topic() + "\t: " + metadata.partition() + ", " + metadata.offset();
    }

    private static String format(ConsumerRecord<?, ?> record) {
        return record.offset() + "\t" + record.key() + "\t: " + record.value();
    }
}
