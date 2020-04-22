package cc.whohow.kafka.util;

import java.util.Scanner;

public class IO {
    public static String readLine() {
        return new Scanner(System.in).nextLine().trim();
    }

    public static String readLines() {
        Scanner scanner = new Scanner(System.in);
        scanner.useDelimiter(scanner.nextLine());
        return scanner.next().replaceAll("\r?\n?$", "");
    }
}
