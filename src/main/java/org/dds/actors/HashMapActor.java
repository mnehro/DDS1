package org.dds.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.dds.messages.*;

import java.util.HashMap;
import java.util.Map;

public class HashMapActor extends AbstractActor {
    private final LoggingAdapter LOG = Logging.getLogger(getContext().getSystem(), this);

    public final Map<Integer, Double> fakeDB = new HashMap<>();

    public static Props props() {
        return Props.create(HashMapActor.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AddMessage.class, this::add)
                .match(PutAllMessage.class, this::putALL)
                .match(ReplaceMessage.class, this::replace)
                .match(RemoveMessage.class, this::remove)
                .match(ClearMessage.class, this::clear)
                .match(ListMessage.class, this::list)
                .matchAny(o -> LOG.info("Unknown message type"))
                .build();
    }

    private boolean keyExists(Integer key) {
        return this.fakeDB.containsKey(key);
    }
    private void add(AddMessage message) {
        if (this.keyExists(message.key())) {
            LOG.info(
                    "Key {} already exists in the DB (Add Operation), {}",
                    message.key(), getSender()
            );
            return;
        }
        this.fakeDB.put(message.key(), message.value());
        LOG.info(
                "Added value with key {} and value {} successfully (Add Operation), {}",
                message.key(), message.value(), getSender()
        );
    }

    private void putALL(PutAllMessage message) {
        Map<Integer, Double> newEntries = new HashMap<>();

        for (Map.Entry<Integer, Double> entry : message.values().entrySet()) {
            Integer key = entry.getKey();
            Double value = entry.getValue();
            if (!keyExists(key)) {
                newEntries.put(key, value);
            } else {
                LOG.info(
                        "Key {} Already exists (Put ALl Operation), {}",
                        key, getSender()
                );
            }
        }
        if (newEntries.isEmpty()) {
            LOG.info(
                    "All keys {} Already exist in the DB (Put ALl Operation), {}",
                    message.values().keySet(), getSender()
            );
            return;
        }
        this.fakeDB.putAll(newEntries);
        LOG.info(
                "Added values with keys {} and values {} successfully (Put ALl Operation), {}",
                newEntries.keySet(), newEntries.values(), getSender()
        );
    }

    private void replace(ReplaceMessage message) {
        if (this.keyExists(message.key())) {
            this.fakeDB.replace(message.key(), message.newValue());
            LOG.info(
                    "Replaced value with key {} with new value {} successfully (Replace Operation), {}",
                    message.key(), message.newValue(), getSender()
            );
            return;
        }
        LOG.info(
                "Key {} Does not exist (Replace Operation), {}",
                message.key(), getSender()
        );
    }

    private void remove(RemoveMessage message) {
        if (this.keyExists(message.key())) {
            this.fakeDB.remove(message.key());
            LOG.info(
                    "Removed value with key {} successfully (Remove Operation), {}",
                    message.key(), getSender()
            );
            return;
        }
        LOG.info(
                "Key {} Does not exist (Remove Operation), {}",
                message.key(), getSender()
        );
    }

    private void clear(ClearMessage message) {
        this.fakeDB.clear();
        LOG.info(
                "Cleared the FakeDB successfully (Clear Operation), {}", getSender()
        );
    }

    private void list(ListMessage message) {
        LOG.info(
                "(List Operation), {}", getSender()
        );
        if (!this.fakeDB.isEmpty()) {
            this.fakeDB.forEach((key, value) -> LOG.info(
                    "Key {} | Value {} ", key, value
            ));
        } else {
            LOG.info(
                    "DB is empty"
            );
        }

    }
}
