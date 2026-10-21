package dev.cascade.compatibility;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;

public final class KafkaJavaSmoke {
  private static final int RECORDS = 25;

  private KafkaJavaSmoke() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      throw new IllegalArgumentException("usage: KafkaJavaSmoke <bootstrap> <topic> <expected-client-version>");
    }
    String bootstrap = args[0];
    String topic = args[1];
    String expectedVersion = args[2];
    String actualVersion = AppInfoParser.getVersion();
    require(actualVersion.equals(expectedVersion),
        "Kafka client version mismatch: expected " + expectedVersion + ", got " + actualVersion);

    String groupId = "cascade-java-matrix-" + expectedVersion.replace('.', '-');
    createTopic(bootstrap, topic);
    produce(bootstrap, topic);
    consumeAndCommit(bootstrap, topic, groupId);
    verifyCommittedOffset(bootstrap, topic, groupId);
    System.out.printf("KAFKA_JAVA_VERSION_RESULT passed version=%s records=%d group=%s%n",
        actualVersion, RECORDS, groupId);
  }

  private static void createTopic(String bootstrap, String topic) throws Exception {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
    try (Admin admin = Admin.create(properties)) {
      admin.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1)))
          .all().get(30, TimeUnit.SECONDS);
    }
  }

  private static void produce(String bootstrap, String topic) throws Exception {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
      for (int index = 0; index < RECORDS; index++) {
        byte[] key = ("key-" + index).getBytes(StandardCharsets.UTF_8);
        byte[] value = ("value-" + index).getBytes(StandardCharsets.UTF_8);
        long offset = producer.send(new ProducerRecord<>(topic, 0, key, value)).get(30, TimeUnit.SECONDS).offset();
        require(offset == index, "unexpected produced offset: " + offset);
      }
      producer.flush();
    }
  }

  private static void consumeAndCommit(String bootstrap, String topic, String groupId) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<String> values = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
      consumer.subscribe(Collections.singleton(topic));
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (values.size() < RECORDS && System.nanoTime() < deadline) {
        consumer.poll(Duration.ofMillis(250)).forEach(record -> {
          require(record.offset() == values.size(), "non-contiguous consumed offset: " + record.offset());
          values.add(new String(record.value(), StandardCharsets.UTF_8));
        });
      }
      List<String> expected = new ArrayList<>();
      for (int index = 0; index < RECORDS; index++) expected.add("value-" + index);
      require(values.equals(expected), "exact Java client consume failed: " + values.size());
      consumer.commitSync();
    }
  }

  private static void verifyCommittedOffset(String bootstrap, String topic, String groupId) throws Exception {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    try (Admin admin = Admin.create(properties)) {
      Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
          .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
      OffsetAndMetadata committed = offsets.get(new TopicPartition(topic, 0));
      require(committed != null && committed.offset() == RECORDS,
          "committed Java client offset is not exact");
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalStateException(message);
  }
}
