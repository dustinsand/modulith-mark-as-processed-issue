package com.example;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.modulith.events.IncompleteEventPublications;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Import(TestModulithMarkAsProcessedIssueApplication.class)
class ModulithMarkAsProcessedIssueApplicationTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(ModulithMarkAsProcessedIssueApplicationTests.class);

    @Autowired
    private SomeService someService;

    @Autowired
    private IncompleteEventPublications incompleteEventPublications;

    @Autowired
    private SomeEventListener someEventListener;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void contextLoads() {
        // execute action that triggers event
        someService.doSomething();

        // wait until event listener runs
        await().untilAsserted(() -> {
            assertThat(someEventListener.getCounter().get()).isOne();
        });

        // listener on purpose fails the first run
        assertThat(incompleteEventCount()).isOne();

        assertThat(getSerializedEvent()).isEqualTo("{\"id\":1,\"eventName\":\"eventOne\"}");
        // Reversing the JSON field order to simulate this issue, https://github.com/spring-projects/spring-modulith/issues/1049
        updateSerializedEvent(jdbcClient);
        assertThat(getSerializedEvent()).isEqualTo("{\"eventName\":\"eventOne\",\"id\":1}");

        // resubmit event - it is expected to succeed
        incompleteEventPublications.resubmitIncompletePublications(eventPublication -> true);

        // wait until it succeeds
        await().untilAsserted(() -> {
            assertThat(someEventListener.isSuccess()).isTrue();
        });

        try {
            // number of incomplete events should be zero
            assertThat(incompleteEventCount()).isZero();
        } catch (AssertionFailedError e) {
            logSerializedEvent();
            throw e;
        }
    }

    private Long incompleteEventCount() {
        return jdbcClient.sql("select count(*) from event_publication where completion_date is null")
                .query(Long.class)
                .single();
    }

    private void logSerializedEvent() {
        // but it is not and there's nothing in regular logs saying that something went wrong
        // except the trace "SQL update affected 0 rows" when
        // UPDATE EVENT_PUBLICATION
        // SET COMPLETION_DATE = ?
        // WHERE
        //		LISTENER_ID = ?
        //		AND SERIALIZED_EVENT = ?
        // gets executed
        String serializedEvent = jdbcClient.sql("select serialized_event from event_publication")
                .query(String.class)
                .single();
        LOGGER.warn("The actual content of serialized_event in the event_publication table: {} most likely does not match the content used when marking event as processed (see trace logs)", serializedEvent);
    }

    public String getSerializedEvent() {
        return jdbcClient.sql("select serialized_event from event_publication")
                .query(String.class)
                .single();
    }

    public void updateSerializedEvent(JdbcClient jdbcClient) {
        String sql = "UPDATE event_publication SET serialized_event = ?";

        int rowsAffected = jdbcClient.sql(sql)
                .params("{\"eventName\":\"eventOne\",\"id\":1}") // Reversing field order from original event
                .update();

        System.out.println("Rows affected: " + rowsAffected);
    }
}
