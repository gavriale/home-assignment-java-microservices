package com.alex.messaging.api.adapter.out.kafka;

import com.alex.messaging.api.config.ReplyProperties;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Picks one fixed partition of the reply topic for this instance to own for its lifetime.
 * Without this, two MS-1 instances sharing one consumer group on the reply topic would have
 * replies land on whichever instance Kafka happened to assign the partition to — not
 * necessarily the one waiting for it (CLAUDE.md §5.3).
 * <p>
 * TRADEOFF: the instance id is the container hostname (unique per container under Compose/K8s)
 * hashed mod the partition count, not a centrally coordinated index. Two instances could in
 * theory hash to the same partition; a production system would instead hand out partitions
 * from a coordinator (e.g. a StatefulSet ordinal). Not worth the extra moving part here —
 * message-api is not the service this exercise scales out (see README/§8, which scales
 * message-processor), so this path is never exercised concurrently in the demo.
 */
@Component
public class ReplyPartitionResolver {

    private final int partition;

    public ReplyPartitionResolver(ReplyProperties properties) {
        this.partition = Math.floorMod(resolveInstanceId().hashCode(), properties.partitionCount());
    }

    public int partition() {
        return partition;
    }

    private static String resolveInstanceId() {
        String hostname = System.getenv("HOSTNAME");
        return (hostname != null && !hostname.isBlank()) ? hostname : UUID.randomUUID().toString();
    }
}
