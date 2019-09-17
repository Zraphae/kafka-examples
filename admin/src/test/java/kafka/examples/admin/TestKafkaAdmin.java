package kafka.examples.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;


@Slf4j
public class TestKafkaAdmin {

    private static final String NEW_TOPIC = "topic-test1";
    private static final String BROKERS_URI = "localhost:9092";

    private static AdminClient adminClient;

    @BeforeClass
    public static void beforeClass(){
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKERS_URI);
        adminClient = AdminClient.create(properties);
    }

    @AfterClass
    public static void afterClass(){
        adminClient.close();
    }

    @Test
    public void createTopics() throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(NEW_TOPIC,2, (short) 1);
        Collection<NewTopic> newTopicList = new ArrayList<>();
        newTopicList.add(newTopic);
        Void aVoid = adminClient.createTopics(newTopicList).all().get();
    }

    @Test
    public void deleteTopics() throws ExecutionException, InterruptedException {
        Void test = adminClient.deleteTopics(Arrays.asList(NEW_TOPIC)).all().get();
        log.info("==>{}", test);
    }

    @Test
    public void listTopics() {

        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<Collection<TopicListing>> kafkaFuture = listTopicsResult.listings();
        Collection<TopicListing> collections;
        List<String> topics = null;
        try {
            collections = kafkaFuture.get();
            if (collections != null && collections.size() != 0) {
                topics = new ArrayList<>(collections.size());
                for (TopicListing topicListing : collections) {
                    topics.add(topicListing.name());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        log.info("===> topics: {}", topics);
    }

    @Test
    public void listTopicsDetail() {

        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(NEW_TOPIC, "test"));
        try {
            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
            TopicDescription topicDescription = stringTopicDescriptionMap.get(NEW_TOPIC);
            log.info("==>topic: {}, partitionNum: {}, replicas: {}", topicDescription.name(),
                    topicDescription.partitions().size(), topicDescription.partitions().get(0).replicas().size());
        } catch (InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
    }

















}
