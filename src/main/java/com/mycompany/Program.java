package com.mycompany;

import com.google.common.util.concurrent.*;
import com.fasterxml.jackson.annotation.*;
import com.satori.rtm.*;
import com.satori.rtm.auth.*;
import com.satori.rtm.model.*;
import java.util.*;
import java.util.concurrent.*;

public class Program {
  static private final String endpoint = "YOUR_ENDPOINT";
  static private final String appkey = "YOUR_APPKEY";
  // Role and secret are optional: replace only if you need to authenticate.
  static private final String role = "YOUR_ROLE";
  static private final String roleSecretKey = "YOUR_SECRET";

  static private final String channel = "animals";

  public static void main(String[] args) throws InterruptedException {
    final RtmClientBuilder builder = new RtmClientBuilder(endpoint, appkey)
        .setListener(new RtmClientAdapter() {
          @Override
          public void onConnectingError(RtmClient client, Exception ex) {
            String msg = String.format("RTM client failed to connect to '%s': %s",
                endpoint, ex.getMessage());
            System.out.println(msg);
          }

          @Override
          public void onError(RtmClient client, Exception ex) {
            String msg = String.format("RTM client failed: %s", ex.getMessage());
            System.out.println(msg);
          }

          @Override
          public void onEnterConnected(RtmClient client) {
            System.out.println("Connected to Satori!");
          }
        });

    //check if the role is set to authenticate or not
    boolean shouldAuthenticate = !"YOUR_ROLE".equals(role);
    if (shouldAuthenticate) {
      builder.setAuthProvider(new RoleSecretAuthProvider(role, roleSecretKey));
    }


    final RtmClient client = builder.build();

    System.out.println(String.format(
        "RTM connection config:\n" +
            "\tendpoint='%s'\n" +
            "\tappkey='%s'\n" +
            "\tauthenticate?=%b", endpoint, appkey, shouldAuthenticate));

    client.start();

    // At this point, the client may not yet be connected to Satori RTM.
    // If the client is not connected, the SDK internally queues the subscription request and
    // will send it once the client connects
    client.createSubscription(channel, SubscriptionMode.SIMPLE,
        new SubscriptionAdapter() {
          @Override
          public void onEnterSubscribed(SubscribeRequest request, SubscribeReply reply) {
            // when subscription is established (confirmed by RTM)
            System.out.println("Subscribed to the channel: " + channel);
          }

          @Override
          public void onSubscriptionError(SubscriptionError error) {
            // when a subscribe or subscription error occurs
            System.out.println("Failed to subscribe: " + error.getReason());
          }

          @Override
          public void onSubscriptionData(SubscriptionData data) {
            // when incoming messages arrive
            for (AnyJson json : data.getMessages()) {
              try {
                // try to convert incoming message to Animal object
                Animal animal = json.convertToType(Animal.class);
                System.out.println("Animal is received: " + animal);
              } catch (Exception ex) {
                System.out.println("Failed to parse incoming message: " + json);
              }
            }
          }
        });


    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    // configure timed task to publish a message each 2 seconds
    executor.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        double lat = 34.134358 + Math.random();
        double lon = -118.321506 + Math.random();
        final Animal animal = new Animal("zebra", new double[]{lat, lon});

        // At this point, the client may not yet be connected to Satori RTM.
        // If the client is not connected, the SDK internally queues the publish request and
        // will send it once the client connects
        ListenableFuture<Pdu<PublishReply>> reply = client.publish(channel, animal, Ack.YES);

        Futures.addCallback(reply, new FutureCallback<Pdu<PublishReply>>() {
          public void onSuccess(Pdu<PublishReply> publishReplyPdu) {
            System.out.println("Animal is published: " + animal);
          }

          public void onFailure(Throwable t) {
            System.out.println("Publish request failed: " + t.getMessage());
          }
        });
      }
    }, 0, 2, TimeUnit.SECONDS);
  }

  /**
   * Sample model to publish to RTM.
   *
   * This class represents the following raw json structure:
   * <pre>{@literal
   * {
   *   "who": "zebra",
   *   "where": [34.134358, -118.321506]
   * }
   * }</pre>
   */
  static class Animal {
    @JsonProperty("who")
    String who;

    @JsonProperty("where")
    double[] where;

    Animal() {}

    Animal(String who, double[] where) {
      this.who = who;
      this.where = where;
    }

    @Override
    public String toString() {
      return "Animal{" +
          "who='" + who + '\'' +
          ", where=" + Arrays.toString(where) +
          '}';
    }
  }
}