package org.lenskit.mooc.uu;

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.lenskit.api.Result;
import org.lenskit.api.ResultMap;
import org.lenskit.basic.AbstractItemScorer;
import org.lenskit.data.dao.DataAccessObject;
import org.lenskit.data.entities.CommonAttributes;
import org.lenskit.data.entities.CommonTypes;
import org.lenskit.data.ratings.Rating;
import org.lenskit.results.Results;
import org.lenskit.util.math.Vectors;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * User-user item scorer.
 *
 * @author <a href="http://www.grouplens.org">GroupLens Research</a>
 */
public class SimpleUserUserItemScorer extends AbstractItemScorer {
    private final DataAccessObject dao;
    private final int neighborhoodSize;

    /**
     * Instantiate a new user-user item scorer.
     *
     * @param dao The data access object.
     */
    @Inject
    public SimpleUserUserItemScorer(DataAccessObject dao) {
        this.dao = dao;
        neighborhoodSize = 30;
    }

    @Nonnull
    @Override
    public ResultMap scoreWithDetails(long user, @Nonnull Collection<Long> items) {
        LongSet users = dao.getEntityIds(CommonTypes.USER);

        double targetUserMeanRating = meanRating(getUserRatingVector(user));
        Map<Long, Long2DoubleOpenHashMap> normalizedUserRatings = normalizeUserRatings(users);
        List<Entry<Long, Double>> similarities = calculateSimilarities(user, normalizedUserRatings).entrySet().stream()
                .sorted(Entry.<Long, Double>comparingByValue().reversed())
                .collect(Collectors.toList());

        return Results.newResultMap(
                items.stream()
                        .map(item -> calculateItemScore(item, similarities, normalizedUserRatings, targetUserMeanRating))
                        .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                        .collect(Collectors.toList())
        );
    }

    private Optional<Result> calculateItemScore(long item, List<Entry<Long, Double>> similarities, Map<Long, Long2DoubleOpenHashMap> normalizedUserRatings, double targetUserMeanRating) {
        int contributions = 0;
        double weightedSumOfNeighbors = 0.0;
        double sumOfSimilarities = 0.0;

        for (Entry<Long, Double> neighborSim : similarities) {
            // For each item’s score, use the 30 most similar users who have rated the item and
            // whose similarity to the target user is positive.
            if (contributions >= neighborhoodSize || neighborSim.getValue() <= 0.0) {
                break;
            }
            Long neighborId = neighborSim.getKey();
            Double neighborSimilarity = neighborSim.getValue();

            Map<Long, Double> neighborRatings = normalizedUserRatings.get(neighborId);
            if (neighborRatings.containsKey(item)) {
                weightedSumOfNeighbors += neighborSimilarity * neighborRatings.get(item);
                sumOfSimilarities += neighborSimilarity;
                contributions++;
            }
        }

        // Refuse to score items if there are not at least 2 neighbors to contribute to the item’s score.
        if (contributions >= 2) {
            return Optional.of(Results.create(item, targetUserMeanRating + (weightedSumOfNeighbors / sumOfSimilarities)));
        } else {
            return Optional.empty();
        }
    }

    private Long2DoubleOpenHashMap calculateSimilarities(long user, Map<Long, Long2DoubleOpenHashMap> normalizedUserRatings) {
        Long2DoubleOpenHashMap similarities = new Long2DoubleOpenHashMap();
        Long2DoubleOpenHashMap targetUserRatings = normalizedUserRatings.get(user);

        for (long neighbor : normalizedUserRatings.keySet()) {
            // Skipping self-correlated user
            if (neighbor == user) {
                continue;
            }

            Long2DoubleOpenHashMap neighborRatings = normalizedUserRatings.get(neighbor);
            similarities.put(neighbor, cosineSimilarity(targetUserRatings, neighborRatings));
        }

        return similarities;
    }

    private double cosineSimilarity(Long2DoubleOpenHashMap targetUserRatings, Long2DoubleOpenHashMap neighborRatings) {
        double similarity = Vectors.dotProduct(targetUserRatings, neighborRatings) /
                (Vectors.euclideanNorm(targetUserRatings) * Vectors.euclideanNorm(neighborRatings));
        if (Double.isNaN(similarity)) {
            similarity = 0;
        }
        return similarity;
    }

    private Map<Long, Long2DoubleOpenHashMap> normalizeUserRatings(LongSet users) {
        return users.stream().collect(Collectors.toMap(u -> u, u -> normalizeRatingsVector(getUserRatingVector(u))));
    }

    private Long2DoubleOpenHashMap normalizeRatingsVector(Long2DoubleOpenHashMap ratings) {
        Long2DoubleOpenHashMap results = new Long2DoubleOpenHashMap();

        double meanRating = meanRating(ratings);

        for (long i : ratings.keySet()) {
            results.put(i, ratings.get(i) - meanRating);
        }

        return results;
    }

    private double meanRating(Long2DoubleOpenHashMap ratings) {
        double ratingsSum = 0.0;
        int numberOfRatings = 0;
        for (double r : ratings.values()) {
            ratingsSum += r;
            numberOfRatings++;
        }
        return ratingsSum / numberOfRatings;
    }


    /**
     * Get a user's rating vector.
     *
     * @param user The user ID.
     * @return The rating vector, mapping item IDs to the user's rating
     * for that item.
     */
    private Long2DoubleOpenHashMap getUserRatingVector(long user) {
        List<Rating> history = dao.query(Rating.class)
                .withAttribute(CommonAttributes.USER_ID, user)
                .get();

        Long2DoubleOpenHashMap ratings = new Long2DoubleOpenHashMap();
        for (Rating r : history) {
            ratings.put(r.getItemId(), r.getValue());
        }

        return ratings;
    }

}
