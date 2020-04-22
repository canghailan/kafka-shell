package cc.whohow.kafka.util;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CSV {
    public static List<String> parse(String csv) {
        if (csv == null) {
            throw new IllegalArgumentException();
        }
        return Pattern.compile(",")
                .splitAsStream(csv)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}
