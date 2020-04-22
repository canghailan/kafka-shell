package cc.whohow.kafka.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Props {
    public static Properties load(String path) {
        try (InputStream stream = Files.newInputStream(Paths.get(path))) {
            Properties properties = new Properties();
            properties.load(stream);
            return properties;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Properties sub(Properties properties, String prefix) {
        Properties props = new Properties();
        for (String p : properties.stringPropertyNames()) {
            if (p.startsWith(prefix)) {
                props.setProperty(p.substring(prefix.length()), properties.getProperty(p));
            }
        }
        return props;
    }
}
