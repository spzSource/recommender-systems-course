package org.lenskit.mooc.cbf;

import org.lenskit.data.ratings.Rating;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Build a user profile from all positive ratings.
 */
public class WeightedUserProfileBuilder implements UserProfileBuilder {
    /**
     * The tag model, to get item tag vectors.
     */
    private final TFIDFModel model;

    @Inject
    public WeightedUserProfileBuilder(TFIDFModel m) {
        model = m;
    }

    @Override
    public Map<String, Double> makeUserProfile(@Nonnull List<Rating> ratings) {
        // Create a new vector over tags to accumulate the user profile
        Map<String, Double> profile = new HashMap<>();

        // TODO Normalize the user's ratings
        double mean = ratings.stream().map(Rating::getValue).reduce(0.0, Double::sum) / ratings.size();

        // TODO Build the user's weighted profile

        for (Rating r : ratings) {
            Map<String, Double> itemVector = model.getItemVector(r.getItemId());
            for (Map.Entry<String, Double> entry : itemVector.entrySet()) {
                double tagScore = (r.getValue() - mean) * entry.getValue();
                profile.compute(entry.getKey(), (k, v) -> (v == null) ? tagScore : v + tagScore);
            }
        }

        // The profile is accumulated, return it.
        return profile;
    }
}
