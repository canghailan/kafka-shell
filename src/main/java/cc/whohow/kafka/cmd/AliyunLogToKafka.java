package cc.whohow.kafka.cmd;

import cc.whohow.kafka.util.Props;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class AliyunLogToKafka implements Runnable {
    private static final Logger log = LogManager.getLogger(AliyunLogToKafka.class);
    private final Properties properties;

    public AliyunLogToKafka(Properties properties) {
        this.properties = properties;
    }

    public void run() {
        Properties sourceProps = Props.sub(properties, "source.");
        String sourceProject = properties.getProperty("source.project");
        String sourceEndpoint = properties.getProperty("source.endpoint");
        String sourceAccessKeyId = properties.getProperty("source.accessKeyId");
        String sourceAccessKeySecret = properties.getProperty("source.accessKeySecret");
        String sourceLogStore = properties.getProperty("source.logStore");
        String sourceConsumerGroup = properties.getProperty("source.consumerGroup", "kafka");
        String sourceConsumer = properties.getProperty("source.consumer", UUID.randomUUID().toString());
        Properties sinkProps = Props.sub(properties, "sink.");

        LogHubConfig logHubConfig = new LogHubConfig(
                sourceConsumerGroup, sourceConsumer,
                sourceEndpoint, sourceProject, sourceLogStore,
                sourceAccessKeyId, sourceAccessKeySecret,
                LogHubConfig.ConsumePosition.BEGIN_CURSOR);
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(sinkProps, new ByteArraySerializer(), new ByteArraySerializer())) {
            while (true) {
                ClientWorker worker = null;
                try {
                    worker = new ClientWorker(new KafkaProcessorFactory(sourceProps, producer), logHubConfig);
                    worker.run();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    if (worker != null) {
                        worker.shutdown();
                    }
                }
            }
        }
    }

    private static class KafkaProcessorFactory implements ILogHubProcessorFactory {
        private final Properties properties;
        private final Producer<byte[], byte[]> kafkaProducer;

        private KafkaProcessorFactory(Properties properties, Producer<byte[], byte[]> kafkaProducer) {
            this.properties = properties;
            this.kafkaProducer = kafkaProducer;
        }

        @Override
        public ILogHubProcessor generatorProcessor() {
            return new KafkaProcessor(properties, kafkaProducer);
        }
    }

    private static class KafkaProcessor implements ILogHubProcessor {
        private final String project;
        private final String logStore;
        private final String logKey;
        private final Producer<byte[], byte[]> kafkaProducer;
        private volatile int shardId;

        private KafkaProcessor(Properties properties, Producer<byte[], byte[]> kafkaProducer) {
            this.project = properties.getProperty("project");
            this.logStore = properties.getProperty("logStore");
            this.logKey = properties.getProperty("logKey", "content");
            this.kafkaProducer = kafkaProducer;
        }

        @Override
        public void initialize(int shardId) {
            this.shardId = shardId;
        }

        @Override
        public String process(List<LogGroupData> list, ILogHubCheckPointTracker iLogHubCheckPointTracker) {
            try {
                for (LogGroupData logGroupData : list) {
                    FastLogGroup fastLogGroup = logGroupData.GetFastLogGroup();
                    String topic = fastLogGroup.getTopic();
                    for (int i = 0; i < fastLogGroup.getLogsCount(); i++) {
                        FastLog fastLog = fastLogGroup.getLogs(i);
                        for (int j = 0; j < fastLog.getContentsCount(); j++) {
                            FastLogContent fastLogContent = fastLog.getContents(j);
                            if (logKey.equals(fastLogContent.getKey())) {
                                try {
                                    RecordMetadata recordMetadata = kafkaProducer.send(
                                            new ProducerRecord<>(topic, null, fastLogContent.getValueBytes())).get();
                                    log.debug("{} {} {} {}({}) -> {}({}, {})",
                                            project, logStore, topic, logKey, shardId,
                                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                                } catch (Exception e) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                        }
                    }
                }
                iLogHubCheckPointTracker.saveCheckPoint(false);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            return null;
        }

        @Override
        public void shutdown(ILogHubCheckPointTracker iLogHubCheckPointTracker) {
            try {
                iLogHubCheckPointTracker.saveCheckPoint(true);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
