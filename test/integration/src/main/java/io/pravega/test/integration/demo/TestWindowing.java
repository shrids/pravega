package io.pravega.test.integration.demo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestWindowing {
    public static void main(String[] args) {

        List<Integer> pravegaStream = Arrays.asList(1, 2, 3, 4, 5);

        // Demonstrate the API that can enable windowing on a Pravega Stream.
        List<Double> average_list = CustomWindowIterator.windowed(pravegaStream, 3)
                                                        .map(s -> s.collect(Collectors.toList()))
                                                        .peek(integers -> System.out
                                                                .println("Window contents " + integers))
                                                        // find the average of the window.
                                                        .map(window -> window.stream()
                                                                             .collect(Collectors
                                                                                     .averagingInt(Integer::intValue)))
                                                        // list all averages
                                                        .collect(Collectors.toList());
        System.out.println("Average list " + average_list);

    }
}
