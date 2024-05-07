package org.dds;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.dds.actors.HashMapActor;
import org.dds.messages.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private final static String ACTOR_SYSTEM = "DDS_1";
    private final static String HASH_MAP_ACTOR = "hashMapActor";

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create(ACTOR_SYSTEM);
        ActorRef hashMapActor = system.actorOf(HashMapActor.props(), HASH_MAP_ACTOR);

        AddMessage addMessage = new AddMessage(1, 22.2);

        Map<Integer, Double> newData = new HashMap<>();
        newData.put(1, 200.2);
        newData.put(3, 500.2);
        newData.put(4, 400.2);

        PutAllMessage putAllMessage = new PutAllMessage(newData);

        ReplaceMessage replaceMessage = new ReplaceMessage(4, 800.2);

        RemoveMessage removeMessage = new RemoveMessage(1);

        ClearMessage clearMessage = new ClearMessage();


        hashMapActor.tell(addMessage, system.guardian());

        hashMapActor.tell(putAllMessage, system.guardian());

        hashMapActor.tell(replaceMessage, system.guardian());

        hashMapActor.tell(removeMessage, system.guardian());

//        hashMapActor.tell(clearMessage, system.guardian());

        system.scheduler().scheduleWithFixedDelay(
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                hashMapActor,
                new ListMessage(),
                system.dispatcher(),
                system.guardian()
        );

    }
}