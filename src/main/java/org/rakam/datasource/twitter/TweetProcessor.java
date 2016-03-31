package org.rakam.datasource.twitter;

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
import java.util.stream.Collectors;

class TweetProcessor implements StatusListener {
    private final Classify classifier = new Classify();
    private final EventApi eventApi;
    private final EventContext eventContext;
    private final Queue<Event> buffer;
    private static final int BUFFER_SIZE = 10;

    public TweetProcessor() {
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath("http://127.0.0.1:9999");
        eventApi = new EventApi(apiClient);

        buffer = new ConcurrentLinkedQueue<>();
        eventContext = new EventContext();
        eventContext.setWriteKey("d4qurtmeffuiika2fik27nr0e2b7cikg7nhc9s91a3gbkmbh832olnjsfh20qar9");
    }

    @Override
    public void onStatus(Status status) {
        Map<String, Object> map = new HashMap<>();

        GeoLocation geoLocation = status.getGeoLocation();
        if(geoLocation != null) {
            map.put("latitude", geoLocation.getLatitude());
            map.put("longitude", geoLocation.getLongitude());
        }

        map.put("_time", status.getCreatedAt().getTime()/ BUFFER_SIZE);
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

        Event event = new Event();
        event.setProperties(map);
        event.setCollection("tweet");
        buffer.add(event);

        commitIfNecessary();
    }

    private synchronized void commitIfNecessary() {
        int size = buffer.size();
        if (size > BUFFER_SIZE) {
            try {
                EventList eventList = new EventList();
                eventList.setProject("twitter");
                eventList.setApi(eventContext);
                Event[] events = new Event[size];
                for (int i = 0; i < size; i++) {
                    events[i] = buffer.poll();
                }
                eventList.setEvents(Arrays.asList(events));
                eventApi.batchEvents(eventList);
                System.out.println("Sent " + size + " events");
            } catch (ApiException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//        Map<String, Object> map = new HashMap<>(2);
//        map.put("_user", statusDeletionNotice.getUserId());
//        map.put("id", statusDeletionNotice.getStatusId());
//
//        Event event = new Event();
//        event.setProperties(map);
//        event.setCollection("delete_tweet");
//        buffer.add(event);
//
//        commitIfNecessary();
    }

    @Override
    public void onTrackLimitationNotice(int limit) {
        System.out.println(1);
    }

    @Override
    public void onScrubGeo(long user, long upToStatus) {
        Map<String, Object> map = new HashMap<>(2);
        map.put("_user", user);
        map.put("id", upToStatus);

        Event event = new Event();
        event.setProperties(map);
        event.setCollection("delete_tweet");
        buffer.add(event);

        commitIfNecessary();
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        System.out.println(1);
    }

    @Override
    public void onException(Exception e) {
        System.out.println(1);
    }
}
