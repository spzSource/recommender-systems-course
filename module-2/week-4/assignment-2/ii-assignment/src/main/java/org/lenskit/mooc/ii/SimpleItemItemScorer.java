package org.lenskit.mooc.ii;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import org.lenskit.api.Result;
import org.lenskit.api.ResultMap;
import org.lenskit.basic.AbstractItemScorer;
import org.lenskit.data.dao.DataAccessObject;
import org.lenskit.data.entities.CommonAttributes;
import org.lenskit.data.ratings.Rating;
import org.lenskit.results.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author <a href="http://www.grouplens.org">GroupLens Research</a>
 */
public class SimpleItemItemScorer extends AbstractItemScorer {
    private final SimpleItemItemModel model;
    private final DataAccessObject dao;
    private final int neighborhoodSize;

    @Inject
    public SimpleItemItemScorer(SimpleItemItemModel m, DataAccessObject dao) {
        model = m;
        this.dao = dao;
        neighborhoodSize = 20;
    }

    /**
     * Score items for a user.
     *
     * @param user  The user ID.
     * @param items The score vector.  Its key domain is the items to score, and the scores
     *              (rating predictions) should be written back to this vector.
     */
    @Override
    public ResultMap scoreWithDetails(long user, @Nonnull Collection<Long> items) {
        Long2DoubleMap itemMeans = model.getItemMeans();
        Long2DoubleMap ratings = getUserRatingVector(user);

        Long2DoubleMap normRatings = normalizeRatings(ratings, itemMeans);

        List<Result> results = new ArrayList<>();

        for (long item : items) {
            results.add(calculateItemScore(item, model.getNeighbors(item), normRatings, itemMeans.get(item)));
        }

        return Results.newResultMap(results);

    }

    private Long2DoubleMap normalizeRatings(Long2DoubleMap ratings, Long2DoubleMap itemMeans) {
        Long2DoubleMap normRatings = new Long2DoubleOpenHashMap();
        for (Map.Entry<Long, Double> entry : ratings.entrySet()) {
            long item = entry.getKey();
            double rating = entry.getValue();
            double mean = itemMeans.get(item);
            normRatings.put(item, rating - mean);
        }
        return normRatings;
    }

    private Result calculateItemScore(
            long item,
            Long2DoubleMap neighbors,
            Long2DoubleMap normalizedRatings,
            double itemMeanRating) {

        int contributions = 0;
        double weightedSumOfNeighbors = 0.0;
        double sumOfSimilarities = 0.0;

        for (Map.Entry<Long, Double> neighborSim : neighbors.entrySet().stream()
                .filter(e -> normalizedRatings.containsKey(e.getKey()))
                .sorted(Map.Entry.<Long, Double>comparingByValue().reversed())
                .collect(Collectors.toList())) {
            // Use at most 20 neighbors to score each item
            if (contributions >= neighborhoodSize) {
                break;
            }
            Long neighborId = neighborSim.getKey();
            Double neighborSimilarity = neighborSim.getValue();

            weightedSumOfNeighbors += neighborSimilarity * normalizedRatings.get(neighborId);
            sumOfSimilarities += neighborSimilarity;
            contributions++;
        }

        return Results.create(item, itemMeanRating + (weightedSumOfNeighbors / sumOfSimilarities));
    }


    /**
     * Get a user's ratings.
     *
     * @param user The user ID.
     * @return The ratings to retrieve.
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
