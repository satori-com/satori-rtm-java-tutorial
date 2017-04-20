package com.mycompany;

import com.satori.rtm.*;
import com.satori.rtm.model.*;
import java.util.Arrays;
import java.util.concurrent.*;

public class App {
  static final String endpoint = "YOUR_ENDPOINT";
  static final String appkey = "YOUR_APPKEY";
  static final String channel = "where.abouts";

  public static void main(String[] args) throws InterruptedException {

    final RtmClient client = new RtmClientBuilder(endpoint, appkey)
        .setListener(new RtmClientAdapter() {
          @Override
          public void onConnectingError(RtmClient client, Exception ex) {
            String msg = String.format("Failed to connect to '%s': %s", endpoint, ex.getMessage());
            System.out.println(msg);
          }

          @Override
          public void onEnterConnected(RtmClient client) {
            System.out.println("Connected to Satori!");
          }
        })
        .build();

    final CountDownLatch success = new CountDownLatch(1);

    client.createSubscription(channel, SubscriptionMode.SIMPLE,
        new SubscriptionAdapter() {
          @Override
          public void onEnterSubscribed(SubscribeRequest request, SubscribeReply reply) {
            Event ev = new Event("zebra", new float[]{34.134358f, -118.321506f});
            client.publish(channel, ev, Ack.NO);
          }

          @Override
          public void onSubscriptionError(SubscriptionError error) {
            System.out.println("Failed to subscribe: " + error.getError());
          }

          @Override
          public void onSubscriptionData(SubscriptionData data) {
            for (Event ev : data.getMessagesAsType(Event.class)) {
              System.out.println("Got message: " + ev);
            }
            success.countDown();
          }
        });

    client.start();

    success.await(15, TimeUnit.SECONDS);

    client.removeSubscription(channel);

    client.shutdown();

    System.out.println("Done. Bye!");
  }

  /**
   * Sample model to publish to RTM.
   *
   * This class represents the following raw json structure:
   * {
   *   "who": "zebra",
   *   "where": [34.134358,-118.321506]
   * }
   */
  static class Event {
    String who;
    float[] where;

    Event() {}

    Event(String who, float[] where) {
      this.who = who;
      this.where = where;
    }

    @Override
    public String toString() {
      return "Event{" +
          "who='" + who + '\'' +
          ", where=" + Arrays.toString(where) +
          '}';
    }
  }
}