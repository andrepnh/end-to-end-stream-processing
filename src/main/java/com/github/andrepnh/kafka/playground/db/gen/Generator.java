package com.github.andrepnh.kafka.playground.db.gen;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.RandomAccess;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.collector.Collectors2;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.tuple.Tuples;

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

  public static Pair<Float, Float> randomLocation() {
    return LocationGenerator.randomLocation();
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

  public static <T> T choose(List<T> options) {
    return options.get(RNG.nextInt(options.size()));
  }

  public static <T> T choose(ImmutableList<T> options) {
    return options.get(RNG.nextInt(options.size()));
  }

  private static class LocationGenerator {
    private static final float LATITUDE_BUCKET_SIZE = 0.1F;

    private static final float LONGITUDE_BUCKET_SIZE = 0.1F;

    private static final float MIN_LATITUDE;

    private static final float MAX_LATITUDE;

    private static final float MIN_LONGITUDE;

    private static final float MAX_LONGITUDE;

    private static final ImmutableList<Range<Float>> LATITUDE_BOUNDS_BY_LONGITUDE_BUCKET;

    private static final ImmutableList<Range<Float>> LONGITUDE_BOUNDS_BY_LATITUDE_BUCKET;

    private static final ImmutableList<UsLocation> FILTERED_LOCATIONS;

    static {
      // Florida's peninsula doesn't suit this generation algorithm
      var outlyingStates = Sets.newHashSet("ALASKA", "HAWAII", "FLORIDA");
      FILTERED_LOCATIONS = Stream.of(UsLocation.values())
          .filter(location -> !outlyingStates.contains(location.getState().trim().toUpperCase()))
          .collect(Collectors2.toImmutableList());

      DoubleSummaryStatistics latitudeSummary = FILTERED_LOCATIONS.stream()
          .mapToDouble(UsLocation::getLatitude)
          .summaryStatistics();
      MIN_LATITUDE = (float) latitudeSummary.getMin();
      MAX_LATITUDE = (float) latitudeSummary.getMax();

      DoubleSummaryStatistics longitudeSummary = FILTERED_LOCATIONS.stream()
          .mapToDouble(UsLocation::getLongitude)
          .summaryStatistics();
      MIN_LONGITUDE = (float) longitudeSummary.getMin();
      MAX_LONGITUDE = (float) longitudeSummary.getMax();

      LATITUDE_BOUNDS_BY_LONGITUDE_BUCKET = groupLatitudeBoundsByLongitudeBuckets();
      LONGITUDE_BOUNDS_BY_LATITUDE_BUCKET = groupLongitudeBoundsByLatitudeBuckets();
    }

    public static Pair<Float, Float> randomLocation() {
      var location = Generator.choose(FILTERED_LOCATIONS);
      float latitude, longitude;
      do {
        float offset = (float) RNG
            .ints(-100000, 100000)
            .limit(1)
            .sum() / 100000;
        latitude = location.getLatitude() + offset;
        offset = (float) RNG
            .ints(-100000, 100000)
            .limit(1)
            .sum() / 100000;
        longitude = location.getLongitude() + offset;
      } while (!withinBounds(latitude, longitude));

      return Tuples.pair(latitude, longitude);
    }

    private static ImmutableList<Range<Float>> groupLongitudeBoundsByLatitudeBuckets() {
      int latitudeBuckets = (int) Math
          .ceil((MAX_LATITUDE - MIN_LATITUDE) / LATITUDE_BUCKET_SIZE) + 1;
      List<Range<Float>> longitudeBoundsByLatitudeBucket = Stream
          .generate(() -> Range.singleton((MAX_LONGITUDE - MIN_LONGITUDE) / 2))
          .limit(latitudeBuckets)
          .collect(Collectors.toList());

      for (var location: FILTERED_LOCATIONS) {
        int latitudeBucket = getLatitudeBucket(location.getLatitude());
        var currBounds = longitudeBoundsByLatitudeBucket.get(latitudeBucket);
        longitudeBoundsByLatitudeBucket
            .set(latitudeBucket, currBounds.span(Range.singleton(location.getLongitude())));
      }

      return Lists.immutable.ofAll(longitudeBoundsByLatitudeBucket);
    }

    private static ImmutableList<Range<Float>> groupLatitudeBoundsByLongitudeBuckets() {
      int longitudeBuckets = (int) Math
          .ceil((MAX_LONGITUDE - MIN_LONGITUDE) / LONGITUDE_BUCKET_SIZE) + 1;
      List<Range<Float>> latitudeBoundsByLongitudeBucket = Stream
          .generate(() -> Range.singleton((MAX_LATITUDE - MIN_LATITUDE) / 2))
          .limit(longitudeBuckets)
          .collect(Collectors.toList());
      for (var location: FILTERED_LOCATIONS) {
        int longitudeBucket = getLongitudeBucket(location.getLongitude());
        var currBounds = latitudeBoundsByLongitudeBucket.get(longitudeBucket);
        latitudeBoundsByLongitudeBucket
            .set(longitudeBucket, currBounds.span(Range.singleton(location.getLatitude())));
      }

      return Lists.immutable.ofAll(latitudeBoundsByLongitudeBucket);
    }

    private static int getLatitudeBucket(float latitude) {
      var bucket = (int) ((latitude - MIN_LATITUDE) / LATITUDE_BUCKET_SIZE);
      checkState(bucket >= 0,
          "Invalid bucket %s for latitude %s. Min latitude: %s; bucket size: %s",
          bucket, latitude, MIN_LATITUDE, LATITUDE_BUCKET_SIZE);
      return bucket;
    }

    private static int getLongitudeBucket(float longitude) {
      int bucket = (int) ((longitude - MIN_LONGITUDE) / LONGITUDE_BUCKET_SIZE);
      checkState(bucket >= 0,
          "Invalid bucket %s for longitude %s. Min longitude: %s; bucket size: %s",
          bucket, longitude, MIN_LONGITUDE, LONGITUDE_BUCKET_SIZE);
      return bucket;
    }

    private static boolean withinBounds(float latitude, float longitude) {
      if (!Range.closed(MIN_LATITUDE, MAX_LATITUDE).contains(latitude) ||
          !Range.closed(MIN_LONGITUDE, MAX_LONGITUDE).contains(longitude)) {
        return false;
      }
      Optional<Range<Float>> latitudeBounds = getLatitudeBoundsByLongitude(longitude);
      var validLatitude = latitudeBounds.map(bounds -> bounds.contains(latitude) ? latitude : null);

      Optional<Range<Float>> longitudeBounds = getLongitudeBoundsByLatitude(latitude);
      var validLongitude = longitudeBounds.map(bounds -> bounds.contains(longitude) ? longitude : null);

      return validLatitude.isPresent() && validLongitude.isPresent();
    }

    private static Optional<Range<Float>> getLongitudeBoundsByLatitude(float latitude) {
      int latitudeBucket = getLatitudeBucket(latitude);
      return latitudeBucket < LONGITUDE_BOUNDS_BY_LATITUDE_BUCKET.size()
          ? Optional.of(LONGITUDE_BOUNDS_BY_LATITUDE_BUCKET.get(latitudeBucket)) : Optional.empty();
    }

    private static Optional<Range<Float>> getLatitudeBoundsByLongitude(float longitude) {
      int longitudeBucket = getLongitudeBucket(longitude);
      return longitudeBucket < LATITUDE_BOUNDS_BY_LONGITUDE_BUCKET.size()
          ? Optional.of(LATITUDE_BOUNDS_BY_LONGITUDE_BUCKET.get(longitudeBucket)) : Optional.empty();
    }

    private LocationGenerator() { }
  }

  private Generator() {}
}
