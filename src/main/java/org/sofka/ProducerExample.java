package org.sofka;

// [START pubsublite_kafka_producer]
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.kafka.ProducerSettings;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerExample {

    public static void main(String... args) throws Exception {
        // TODO(developer): Replace these variables before running the sample.
        String cloudRegion = "us-central1";
        char zoneId = 'a';
        // Use an existing Pub/Sub Lite topic.
        String topicId = "your-lite-topic";
        // Using the project number is required for constructing a Pub/Sub Lite
        // topic path that the Kafka producer can use.
        long projectNumber = Long.parseLong("534260370479");

        producerExample(cloudRegion, zoneId, projectNumber, topicId);
    }

    public static void producerExample(
            String cloudRegion, char zoneId, long projectNumber, String topicId)
            throws InterruptedException, ExecutionException {
        TopicPath topicPath =
                TopicPath.newBuilder()
                        .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
                        .setProject(ProjectNumber.of(projectNumber))
                        .setName(TopicName.of(topicId))
                        .build();

        ProducerSettings producerSettings =
                ProducerSettings.newBuilder().setTopicPath(topicPath).build();

        List<Future<RecordMetadata>> futures = new ArrayList<>();
        try (Producer<byte[], byte[]> producer = producerSettings.instantiate()) {
            for (long i = 0L; i < 10L; i++) {
                String key = "demo";
                Future<RecordMetadata> future =
                        producer.send(
                                new ProducerRecord(
                                        topicPath.toString(), key.getBytes(), ("message-" + i).getBytes()));
                futures.add(future);
            }
            for (Future<RecordMetadata> future : futures) {
                RecordMetadata meta = future.get();
                System.out.println(meta.offset());
            }
        }
        System.out.printf("Published 10 messages to %s%n", topicPath.toString());
    }
}
// [END pubsublite_kafka_producer]