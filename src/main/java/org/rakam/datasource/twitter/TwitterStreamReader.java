package org.rakam.datasource.twitter;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
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
import java.util.concurrent.TimeUnit;

public class TwitterStreamReader
{
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
                client, queue, ImmutableList.of(new TweetProcessor(rakamApi, rakamApiKey)),
                executorService);

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

        System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        TwitterStreamReader.run(args[0], args[1], args[2], args[3], args[4], args[5]);
    }
}