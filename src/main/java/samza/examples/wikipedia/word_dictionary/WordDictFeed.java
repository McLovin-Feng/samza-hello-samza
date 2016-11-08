package samza.examples.wikipedia.word_dictionary;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Date;

/**
 * Created by fengbo on 10/21/16.
 */
public class WordDictFeed implements StreamTask {

    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "word-dict-feed");

    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        System.out.println(envelope.getMessage());
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, envelope.getMessage() + "\t" + (new Date())));
        /* Commit for every msg */
//        coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    }

}
