/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rakam.datasource.twitter;

import com.google.common.base.Throwables;
import org.rakam.ApiClient;
import org.rakam.ApiException;
import org.rakam.client.api.EventApi;
import org.rakam.client.model.Event;
import org.rakam.client.model.EventContext;
import org.rakam.client.model.EventList;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class TweetProcessor implements StatusListener {
    private final Logger LOGGER = Logger.getLogger(TweetProcessor.class.getName());
    private final Classify classifier = new Classify();
    private final EventApi eventApi;
    private final EventContext eventContext;
    private final Queue<org.rakam.client.model.Event> buffer;
    private static final int BUFFER_SIZE = 50;
    private final AtomicInteger counter;

    public TweetProcessor(String apiUrl, String apiKey, AtomicInteger counter) {
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(apiUrl);
        eventApi = new EventApi(apiClient);
        this.counter = counter;
        buffer = new ConcurrentLinkedQueue<>();
        eventContext = new EventContext();
        eventContext.setWriteKey(apiKey);
    }

    @Override
    public void onStatus(Status status) {
        Map<String, Object> map = new HashMap<>();

        GeoLocation geoLocation = status.getGeoLocation();
        if(geoLocation != null) {
            map.put("latitude", geoLocation.getLatitude());
            map.put("longitude", geoLocation.getLongitude());
        }

        map.put("_time", status.getCreatedAt().getTime());
        Place place = status.getPlace();
        if(place != null) {
            map.put("country_code", place.getCountryCode());
            map.put("place", place.getName());
            map.put("place_type", place.getPlaceType());
            map.put("place_id", place.getId());
        }

        User user = status.getUser();
        map.put("_user", user.getId());
        map.put("user_lang", user.getLang());
        map.put("user_created", user.getCreatedAt());
        map.put("user_followers", user.getFollowersCount());
        map.put("user_status_count", user.getStatusesCount());
        map.put("user_verified", user.isVerified());

        map.put("id", status.getId());
        map.put("is_reply", status.getInReplyToUserId() > -1);
        map.put("is_retweet", status.isRetweet());
        map.put("has_media",  status.getMediaEntities().length > 0);
        map.put("urls", Arrays.stream(status.getURLEntities()).map(URLEntity::getText).collect(Collectors.toList()));
        map.put("hashtags", Arrays.stream(status.getHashtagEntities()).map(HashtagEntity::getText).collect(Collectors.toList()));
        map.put("user_mentions", Arrays.stream(status.getUserMentionEntities()).map(UserMentionEntity::getText).collect(Collectors.toList()));
        map.put("language", "und".equals(status.getLang()) ? null : status.getLang());
        map.put("is_positive", classifier.isPositive(status.getText()));

        Event event = new Event()
                .properties(map)
                .collection("tweet13");
        buffer.add(event);

        commitIfNecessary();
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
    {
    }

    private synchronized void commitIfNecessary() {
        int size = buffer.size();
        if (size > BUFFER_SIZE) {
            try {
                EventList eventList = new EventList()
                        .api(eventContext);
                Event[] events = new Event[size];
                for (int i = 0; i < size; i++) {
                    events[i] = buffer.poll();
                }
                eventList.setEvents(Arrays.asList(events));
                eventApi.batchEvents(eventList);
                counter.addAndGet(size);
            } catch (ApiException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public void onTrackLimitationNotice(int limit) {
        LOGGER.log(Level.WARNING, String.format("We hit the Twitter API limits, maximum %s can be fetched", limit));
    }

    @Override
    public void onScrubGeo(long l, long l1)
    {
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        LOGGER.log(Level.WARNING, String.format("Warning while sending tweets to Rakam: %s", warning.getMessage()));
    }

    @Override
    public void onException(Exception e) {
        LOGGER.log(Level.SEVERE, String.format("Error while sending tweets to Rakam: %s", e.getMessage()));
    }
}
