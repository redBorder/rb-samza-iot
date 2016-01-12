package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;

import java.util.Map;

public class IotProcessor extends Processor {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "rb_iot_post");

    public IotProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, storeManager.enrichFull(message)));
    }

    @Override
    public String getName() {
        return null;
    }
}
