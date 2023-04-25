package rpks;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;
import ru.mai.lessons.rpks.Service;
import ru.mai.lessons.rpks.impl.ServiceEnrichment;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@Testcontainers
class ServiceTest {

    @Container
    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    @Container
    private final JdbcDatabaseContainer<?> postgreSQL = new PostgreSQLContainer(DockerImageName.parse("postgres"))
            .withDatabaseName("test_db")
            .withUsername("user")
            .withPassword("password")
            .withInitScript("init_script.sql");

    @Container
    private final MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:4.0.10"));

    ExecutorService executorForTest = Executors.newFixedThreadPool(2);

    private DataSource dataSource;
    private final static String TEST_TOPIC_IN = "test_topic_in";
    private final static String TEST_TOPIC_OUT = "test_topic_out";
    private final short replicaFactor = 1;
    private final int partitions = 3;

    private final String tableName = "enrichment_rules";

    private final Service serviceEnrichment = new ServiceEnrichment();

    private MongoClient mongoClient;

    private static final List<String> LIST_DEFAULT_DB = List.of("admin", "config", "local");

    public static final String MONGO_TEST_DB = "enrichment_db";
    public static final String MONGO_TEST_COLLECTION = "enrichment_collection";

    private MongoClient getMongoClient() {
        return Optional.ofNullable(mongoClient).orElse(MongoClients.create(mongoDBContainer.getConnectionString()));
    }

    @AfterEach
    void clearMongoAndCloseMongoClient() {
        Optional.ofNullable(mongoClient).ifPresent(mc -> {
            mc.listDatabaseNames().forEach(nameDb -> {
                if (!LIST_DEFAULT_DB.contains(nameDb)) {
                    mongoClient.getDatabase(nameDb).drop();
                }
            });
            mc.close();
        });
    }

    /**
     * Проверяет готовность Kafka
     */
    @Test
    void testStartKafka() {
        assertTrue(kafka.isRunning());
    }

    /**
     * Проверяет готовность postgreSQL
     */
    @Test
    void testStartPostgreSQL() {
        assertTrue(postgreSQL.isRunning());
    }

    /**
     * Проверяет готовность MongoDB
     */
    @Test
    void testStartMongoDB() {
        assertTrue(mongoDBContainer.isRunning());
    }

    /**
     * Проверяет возможность читать и писать из Kafka
     */
    @Test
    void testKafkaWriteReadMessage() {
        log.info("Bootstrap.servers: {}", kafka.getBootstrapServers());
        List<NewTopic> topics = Stream.of(TEST_TOPIC_IN, TEST_TOPIC_OUT)
                .map(topicName -> new NewTopic(topicName, partitions, replicaFactor))
                .toList();

        log.info("Topics: {}, replica factor {}, partitions {}", topics, replicaFactor, partitions);

        try (AdminClient adminClient = createAdminClient();
             KafkaConsumer<String, String> consumer = createConsumer();
             KafkaProducer<String, String> producer = createProducer()) {
            log.info("Creating topics");
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            log.info("Created topics");
            log.info("Sending message");
            producer.send(new ProducerRecord<>(TEST_TOPIC_IN, "testKey", "json")).get();
            log.info("Sent message");

            log.info("Consumer subscribe");
            consumer.subscribe(Collections.singletonList(TEST_TOPIC_IN));
            log.info("Consumer start reading");

            getConsumerRecordsOutputTopic(consumer, 10, 1);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Тест проверяет возможноть чтения данных из PostgreSQL
     */
    @Test
    void testPostgreSQLReadValues() {
        clearTable();
        createAndCheckRuleInPostgreSQL(0L, 0L, "test_field", "test_field_enrichment", "1", "0");
    }

    /**
     * Тест проверяет возможноть чтения и записи данных из MongoDB
     */
    @Test
    void testMongoReadWriteValues() {
        log.info("Connection mongodb {}", mongoDBContainer.getConnectionString());

        mongoClient = getMongoClient();

        var mongoDatabase = mongoClient.getDatabase("test_db");
        log.info("Created mongo db test_db");

        mongoDatabase.createCollection("test_collection");
        log.info("Created mongo collection test_collection in test_db");

        log.info("Create document");
        var testDocument = new Document()
                .append("id", 1)
                .append("fieldName", "testFieldName")
                .append("fieldvValue", "testFieldValue")
                .append("fieldBoolean", true)
                .append("fieldArrays", Arrays.asList("test1", "test2", "test3"));
        log.info("Document created: {}", testDocument.toJson());

        log.info("Get collection");
        var testCollection = mongoDatabase.getCollection("test_collection");
        log.info("Collection: {}", testCollection);

        log.info("Insert document into collection");
        testCollection.insertOne(testDocument);

        var actualDocument = testCollection.find(eq("id", 1)).first();

        assertEquals(testDocument, actualDocument);
    }

    /**
     * Тест проверяет, что сервис обогащения без правил обогащения просто пропускает все сообщения в том виде, в котором они пришли
     * Запускается приложение с тестовыми конфигурациями в test/resources/application.conf.
     * Отправляется несколько сообщений во входной топик.
     * Проверяется выходной топик.
     * Проверяем, что на выходе те же сообщения, что и на входе.
     */
    @Test
    void testServiceEnrichmentNoRules() {
        List<NewTopic> topics = Stream.of(TEST_TOPIC_IN, TEST_TOPIC_OUT)
                .map(topicName -> new NewTopic(topicName, partitions, replicaFactor))
                .toList();

        log.info("Topics: {}, replica factor {}, partitions {}", topics, replicaFactor, partitions);

        try (AdminClient adminClient = createAdminClient();
             KafkaConsumer<String, String> consumer = createConsumer();
             KafkaProducer<String, String> producer = createProducer()) {

            checkAndCreateRequiredTopics(adminClient, topics);

            consumer.subscribe(Collections.singletonList(TEST_TOPIC_OUT));

            clearTable();

            Config config = ConfigFactory.load();
            config = replaceConfigForTest(config);
            Future<Boolean> serviceIsWork = testStartService(config);

            String expectedJsonOne = "{\"name\":\"alex\", \"age\":18, \"sex\":\"M\"}";
            String expectedJsonTwo = "{\"name\":\"no_alex\", \"age\":19, \"sex\":\"F\"}";

            List<String> listExpectedJson = List.of(expectedJsonOne, expectedJsonTwo);

            listExpectedJson.forEach(json -> {
                try {
                    producer.send(new ProducerRecord<>(TEST_TOPIC_IN, "expected", json)).get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error send message to kafka topic", e);
                    fail();
                }
            });

            Future<ConsumerRecords<String, String>> result = executorForTest.submit(() -> getConsumerRecordsOutputTopic(consumer, 10, 1));

            var consumerRecords = result.get(60, TimeUnit.SECONDS);

            assertFalse(consumerRecords.isEmpty());
            assertEquals(2, consumerRecords.count());

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                assertNotNull(consumerRecord.value());
                assertTrue(listExpectedJson.contains(consumerRecord.value()));
            }

            serviceIsWork.cancel(true);
            executorForTest.shutdown();
            clearTable();

        } catch (Exception e) {
            log.error("Error test execution", e);
            fail();
        }
    }

    private AdminClient createAdminClient() {
        return AdminClient.create(ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    private KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
    }

    private KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }

    private HikariDataSource createConnectionPool() {
        var config = new HikariConfig();
        config.setJdbcUrl(postgreSQL.getJdbcUrl());
        config.setUsername(postgreSQL.getUsername());
        config.setPassword(postgreSQL.getPassword());
        config.setDriverClassName(postgreSQL.getDriverClassName());
        return new HikariDataSource(config);
    }

    private void checkAndCreateRequiredTopics(AdminClient adminClient, List<NewTopic> topics) {
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (existingTopics.isEmpty()) {
                log.info("Topic not exist. Create topics {}", topics);
                adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            } else {
                topics.stream().map(NewTopic::name).filter(t -> !existingTopics.contains(t)).forEach(t -> {
                    try {
                        log.info("Topic not exist {}. Create topic {}", t, t);
                        adminClient.createTopics(List.of(new NewTopic(t, partitions, replicaFactor))).all().get(30, TimeUnit.SECONDS);
                    } catch (InterruptedException | TimeoutException | ExecutionException e) {
                        log.error("Error creating topic Kafka", e);
                    }
                });
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Error checking topics", e);
        }
    }

    private void clearTable() {
        try {
            if (dataSource == null) {
                dataSource = createConnectionPool();
            }
            DSLContext context = DSL.using(dataSource.getConnection(), SQLDialect.POSTGRES);
            context.deleteFrom(table(tableName)).execute();

            var result = context.select(
                            field("enrichment_id"),
                            field("rule_id"),
                            field("field_name"),
                            field("field_name_enrichment"),
                            field("field_value"),
                            field("field_value_default")
                    )
                    .from(table(tableName))
                    .fetch();

            assertTrue(result.isEmpty());
        } catch (SQLException ex) {
            log.error("Error truncate table", ex);
        }
    }

    private void createAndCheckRuleInPostgreSQL(Long enrichmentId, Long ruleId, String fieldName, String fieldNameEnrichment, String fieldValue, String fieldValueDefault) {
        try {
            if (dataSource == null) {
                dataSource = createConnectionPool();
            }
            log.info("Create filtering rule 1");
            DSLContext context = DSL.using(dataSource.getConnection(), SQLDialect.POSTGRES);
            context.insertInto(table(tableName)).columns(
                            field("enrichment_id"),
                            field("rule_id"),
                            field("field_name"),
                            field("field_name_enrichment"),
                            field("field_value"),
                            field("field_value_default")
                    ).values(enrichmentId, ruleId, fieldName, fieldNameEnrichment, fieldValue, fieldValueDefault)
                    .execute();

            log.info("Check rule from DB");
            var result = context.select(
                            field("enrichment_id"),
                            field("rule_id"),
                            field("field_name"),
                            field("field_name_enrichment"),
                            field("field_value"),
                            field("field_value_default")
                    )
                    .from(table(tableName))
                    .where(field("enrichment_id").eq(enrichmentId).and(field("rule_id").eq(ruleId)))
                    .fetch();

            String expectedValue =
                    String.format("enrichment_id,rule_id,field_name,field_name_enrichment,field_value,field_value_default\n%d,%d,%s,%s,%s,%s\n",
                            enrichmentId, ruleId, fieldName, fieldNameEnrichment, fieldValue, fieldValueDefault);

            assertEquals(expectedValue, result.formatCSV());
        } catch (SQLException ex) {
            log.error("Error creating rule", ex);
        }
    }

    private ConsumerRecords<String, String> getConsumerRecordsOutputTopic(KafkaConsumer<String, String> consumer, int retry, int timeoutSeconds) {
        boolean state = false;
        try {
            while (!state && retry > 0) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (consumerRecords.isEmpty()) {
                    log.info("Remaining attempts {}", retry);
                    retry--;
                    Thread.sleep(timeoutSeconds * 1000L);
                } else {
                    log.info("Read messages {}", consumerRecords.count());
                    return consumerRecords;
                }
            }
        } catch (InterruptedException ex) {
            log.error("Interrupt read messages", ex);
        }
        return ConsumerRecords.empty();
    }

    private Config replaceConfigForTest(Config config) {
        return config.withValue("kafka.consumer.bootstrap.servers", ConfigValueFactory.fromAnyRef(kafka.getBootstrapServers()))
                .withValue("kafka.producer.bootstrap.servers", ConfigValueFactory.fromAnyRef(kafka.getBootstrapServers()))
                .withValue("db.jdbcUrl", ConfigValueFactory.fromAnyRef(postgreSQL.getJdbcUrl()))
                .withValue("db.user", ConfigValueFactory.fromAnyRef(postgreSQL.getUsername()))
                .withValue("db.password", ConfigValueFactory.fromAnyRef(postgreSQL.getPassword()))
                .withValue("db.driver", ConfigValueFactory.fromAnyRef(postgreSQL.getDriverClassName()))
                .withValue("application.updateIntervalSec", ConfigValueFactory.fromAnyRef(10))
                .withValue("mongo.connectionString", ConfigValueFactory.fromAnyRef(mongoDBContainer.getConnectionString()))
                .withValue("mongo.database", ConfigValueFactory.fromAnyRef(MONGO_TEST_DB))
                .withValue("mongo.collection", ConfigValueFactory.fromAnyRef(MONGO_TEST_COLLECTION));
    }

    private Future<Boolean> testStartService(Config config) {
        return executorForTest.submit(() -> {
            serviceEnrichment.start(config);
            return true;
        });
    }
}