package com.github.andrepnh.kafka.playground.db.gen;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class Generator {
  private static final long MAX_EPOCH_OFFSET_MILLIS =
      Instant.now().minusMillis(Instant.EPOCH.toEpochMilli()).toEpochMilli();

  private static final Random RNG = new Random();

  public static final String LETTERS = "abcdefghijklmnopqrstuvwxyz";

  public static ZonedDateTime moment() {
    var instant = Instant.EPOCH.plusMillis(
        RNG.longs(0, MAX_EPOCH_OFFSET_MILLIS + 1)
            .limit(1)
            .sum());
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC.normalized());
  }

  public static int rangeClosed(int lowerBound, int upperBound) {
    return RNG.ints(lowerBound, upperBound + 1).limit(1).sum();
  }

  public static int positive(int upperBoundInclusive) {
    return RNG.nextInt(upperBoundInclusive) + 1;
  }

  public static String words() {
    return words(5);
  }

  public static String words(int maxWords) {
    return Stream.generate(Generator::word)
        .limit(RNG.nextInt(maxWords) + 1)
        .collect(Collectors.joining(" "));
  }

  public static String word() {
    return word(7);
  }

  public static String word(int maxLength) {
    return IntStream.range(0, RNG.nextInt(maxLength) + 1)
        .mapToObj(i -> letter(i == 0))
        .collect(Collectors.joining());
  }

  public static String letter(boolean upper) {
    var letter = String.valueOf(LETTERS.charAt(RNG.nextInt(LETTERS.length())));
    return upper ? letter.toUpperCase() : letter;
  }

  private Generator() {}
}
