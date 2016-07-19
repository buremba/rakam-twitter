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
import com.google.common.collect.ImmutableList;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.StatsReporter;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TwitterStreamReader
{
    private static final Logger LOGGER = Logger.getLogger(TwitterStreamReader.class.getName());

    private static final AtomicLong total = new AtomicLong();
    private static final AtomicInteger currentMinute = new AtomicInteger();

    public static void run(String rakamApi, String rakamApiKey, String twitterConsumerKey, String twitterConsumerSecret, String twitterToken, String twitterSecret)
            throws InterruptedException
    {
        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(twitterConsumerKey, twitterConsumerSecret, twitterToken, twitterSecret);

        BasicClient client = new ClientBuilder()
                .name("rakam-client")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        int nThreads = Runtime.getRuntime().availableProcessors() * 2;
        ExecutorService executorService = Executors
                .newFixedThreadPool(nThreads);

        Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
                client, queue, ImmutableList.of(new TweetProcessor(rakamApi, rakamApiKey, currentMinute)),
                executorService);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            int currentMinuteEvents = currentMinute.getAndSet(0);
            long totalEvents = total.addAndGet(currentMinuteEvents);
            LOGGER.info(String.format("Total: %s, Last 15 seconds: %d", totalEvents, currentMinuteEvents));
        }, 5, 15, SECONDS);

        t4jClient.connect();
        for (int threads = 0; threads < nThreads; threads++) {
            // This must be called once per processing thread
            t4jClient.process();
        }

        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e) {
            client.stop();
            throw Throwables.propagate(e);
        }

        client.stop();
        scheduledExecutorService.shutdown();

        StatsReporter.StatsTracker statsTracker = client.getStatsTracker();
        LOGGER.info(String.format("API Stats: \n The client read %d messages. \n Status code 200: %d, 400: %d, 500: %s \n Client dropped: %d, Failure: %d, Dropped: %d \n Connect: %d, Connection failure: %d, Disconnect: %d",
                statsTracker.getNumMessages(), statsTracker.getNum200s(), statsTracker.getNum400s(), statsTracker.getNum500s(),
                statsTracker.getNumClientEventsDropped(), statsTracker.getNumConnectionFailures(), statsTracker.getNumMessagesDropped(),
                statsTracker.getNumConnects(), statsTracker.getNumConnectionFailures(), statsTracker.getNumDisconnects()));
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        TwitterStreamReader.run(args[0], args[1], args[2], args[3], args[4], args[5]);
    }
}