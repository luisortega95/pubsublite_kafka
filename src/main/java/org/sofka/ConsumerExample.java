package org.sofka;


// [START pubsublite_kafka_consumer]
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.kafka.ConsumerSettings;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerExample {

    public static void main(String... args) throws Exception {
        // TODO(developer): Replace these variables before running the sample.
        String cloudRegion = "us-central1";
        char zoneId = 'a';
        // Use an existing Pub/Sub Lite topic and subscription.
        String topicId = "your-lite-topic";
        String subscriptionId = "your-lite-subscription";
        // Using the project number here is required for constructing a Pub/Sub Lite
        // topic path that the Kafka consumer can use.
        long projectNumber = Long.parseLong("534260370479");

        consumerExample(cloudRegion, zoneId, projectNumber, topicId, subscriptionId);
    }

    public static void consumerExample(
            String cloudRegion, char zoneId, long projectNumber, String topicId, String subscriptionId) {

        CloudZone location = CloudZone.of(CloudRegion.of(cloudRegion), zoneId);

        TopicPath topicPath =
                TopicPath.newBuilder()
                        .setLocation(location)
                        .setProject(ProjectNumber.of(projectNumber))
                        .setName(TopicName.of(topicId))
                        .build();

        SubscriptionPath subscription =
                SubscriptionPath.newBuilder()
                        .setLocation(location)
                        .setProject(ProjectNumber.of(projectNumber))
                        .setName(SubscriptionName.of(subscriptionId))
                        .build();

        FlowControlSettings flowControlSettings =
                FlowControlSettings.builder()
                        // 50 MiB. Must be greater than the allowed size of the largest message (1 MiB).
                        .setBytesOutstanding(50 * 1024 * 1024L)
                        // 10,000 outstanding messages. Must be >0.
                        .setMessagesOutstanding(10000L)
                        .build();

        ConsumerSettings settings =
                ConsumerSettings.newBuilder()
                        .setSubscriptionPath(subscription)
                        .setPerPartitionFlowControlSettings(flowControlSettings)
                        .setAutocommit(true)
                        .build();

        Set<ConsumerRecord<byte[], byte[]>> hashSet = new HashSet<>();
        try (Consumer<byte[], byte[]> consumer = settings.instantiate()) {
            // The consumer can only subscribe to the topic that it is associated to.
            // If this is the only subscriber for this subscription, it will take up
            // to 90s for the subscriber to warm up.
            consumer.subscribe(Arrays.asList(topicPath.toString()));
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    long offset = record.offset();
                    String value = Base64.getEncoder().encodeToString(record.value());
                    hashSet.add(record);
                    System.out.printf("Received %s: %s%n", offset, value);
                }
                // Early exit. Remove entirely to keep the consumer alive indefinitely.
                if (hashSet.size() >= 10) {
                    System.out.println("Received 10 messages.");
                    break;
                }
            }
        }
    }
}
// [END pubsublite_kafka_consumer]
