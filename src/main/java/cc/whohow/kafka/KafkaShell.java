package cc.whohow.kafka;

import cc.whohow.kafka.cmd.*;
import cc.whohow.kafka.util.Props;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class KafkaShell {
    private static final Logger log = LogManager.getLogger(KafkaShell.class);

    public static void main(String[] args) {
        newCommand(getProps(args)).run();
    }

    private static Properties getProps(String[] args) {
        if (args.length > 0) {
            return Props.load(args[0]);
        }
        if (Files.exists(Paths.get("kafka-shell.properties"))) {
            return Props.load("kafka-shell.properties");
        }
        return System.getProperties();
    }

    private static Runnable newCommand(Properties properties) {
        String source = properties.getProperty("source");
        String sink = properties.getProperty("sink");
        String command = source + "->" + sink;
        log.info(command);
        switch (command) {
            case "kafka->kafka":
                return new KafkaToKafka(properties);
            case "kafka->stdout":
                return new KafkaToStdout(properties);
            case "stdin->kafka":
                return new StdinToKafka(properties);
            case "kafka->aliyun-log":
                return new KafkaToAliyunLog(properties);
            case "aliyun-log->kafka":
                return new AliyunLogToKafka(properties);
            default:
                throw new IllegalArgumentException(command);
        }
    }
}
