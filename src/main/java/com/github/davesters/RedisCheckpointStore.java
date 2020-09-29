package com.github.davesters;

import com.microsoft.azure.eventprocessorhost.BaseLease;
import com.microsoft.azure.eventprocessorhost.Checkpoint;
import com.microsoft.azure.eventprocessorhost.CompleteLease;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

/**
 * A checkpoint store that stores data in Redis.
 */
class RedisCheckpointStore implements CheckpointStore {

    private static final Logger LOG = LoggerFactory.getLogger(RedisCheckpointStore.class.getSimpleName());

    private final RedisCheckpointOptions options;
    private final String hashKey;
    private final String leaseHashKey;
    private final Map<String, String> checkpoints = new HashMap<>();
    private final Object sync = new Object();

    private Timer writeTimer = new Timer();
    private JedisPool pool;

    /**
     * Creates a new instance of RedisCheckpointStore.
     * @param options The options object
     */
    RedisCheckpointStore(RedisCheckpointOptions options) {
        this.options = options;
        this.hashKey = options.getCheckpointKeyName();
        this.leaseHashKey = this.hashKey + "_lease";
    }

    @Override
    public void connect() {
        if (this.options.getRedisHostname() == null || this.options.getRedisHostname().trim().isEmpty()) {
            return;
        }

        this.pool = new JedisPool(
            new JedisPoolConfig(),
            this.options.getRedisHostname(),
            this.options.getPort(),
            this.options.getConnectTimeout(),
            this.options.getPassword(),
            this.options.getDatabase(),
            this.hashKey,
            this.options.isSsl());

        if (this.options.isBatchCheckpointWrites()) {
            this.writeTimer = new Timer();
            this.writeTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    writeCheckpoints();
                }
            }, this.options.getBatchIntervalInMillis(), this.options.getBatchIntervalInMillis());
        }
    }

    @Override
    public boolean connected() {
        return this.pool != null && !this.pool.isClosed();
    }

    @Override
    public Checkpoint getCheckpoint(String partitionId) {
        String result;

        try (Jedis jedis = pool.getResource()) {
            result = jedis.hget(hashKey, partitionId);
        }

        if (result == null) {
            return null;
        }

        String[] segments = result.split(",");

        return new Checkpoint(partitionId, segments[0], Long.parseLong(segments[1]));
    }

    @Override
    public void setCheckpoint(Checkpoint checkpoint) {
        String value = String.format("%s,%d", checkpoint.getOffset(), checkpoint.getSequenceNumber());

        if (this.options.isBatchCheckpointWrites()) {
            synchronized (sync) {
                checkpoints.put(checkpoint.getPartitionId(), value);
            }
        } else {
            try (Jedis jedis = pool.getResource()) {
                jedis.hset(hashKey, checkpoint.getPartitionId(), value);
            }
        }
    }

    private void writeCheckpoints() {
        if (pool == null || pool.isClosed() || checkpoints.size() == 0) {
            return;
        }

        try (Jedis jedis = pool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            LOG.debug("writing {} checkpoints to redis", checkpoints.size());

            synchronized (sync) {
                checkpoints.forEach((key, value) -> pipeline.hset(hashKey, key, value));
                checkpoints.clear();
            }

            if (pool != null && !pool.isClosed()) {
                pipeline.sync();
            }
        }
    }

    @Override
    public void deleteCheckpoint(String partitionId) {
        try (Jedis jedis = pool.getResource()) {
            jedis.hdel(hashKey, partitionId);
        }
    }

    @Override
    public Map<String, Checkpoint> getAllCheckpoints() {
        try (Jedis jedis = pool.getResource()) {
            Map<String, String> values = jedis.hgetAll(hashKey);

            return values.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    String[] segments = entry.getValue().split(",");
                    return new Checkpoint(entry.getKey(), segments[0], Long.parseLong(segments[1]));
                }));
        }
    }

    @Override
    public CompleteLease getLease(String partitionId) {
        String result;

        try (Jedis jedis = pool.getResource()) {
            result = jedis.hget(leaseHashKey, partitionId);
        }

        if (result == null) {
            return null;
        }

        return new RedisLease(partitionId, result);
    }

    @Override
    public void setLease(CompleteLease lease) {
        RedisLease leaseToSet = (RedisLease) lease;

        try (Jedis jedis = pool.getResource()) {
            jedis.hset(leaseHashKey, lease.getPartitionId(), leaseToSet.getRedisValue());
        }
    }

    @Override
    public void deleteLease(String partitionId) {
        try (Jedis jedis = pool.getResource()) {
            jedis.hdel(leaseHashKey, partitionId);
        }
    }

    @Override
    public Map<String, CompleteLease> getAllLeases() {
        try (Jedis jedis = pool.getResource()) {
            Map<String, String> values = jedis.hgetAll(leaseHashKey);

            return values.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                    entry -> new RedisLease(entry.getKey(), entry.getValue())));
        }
    }

    @Override
    public List<BaseLease> getBaseLeases() {
        try (Jedis jedis = pool.getResource()) {
            Map<String, String> values = jedis.hgetAll(leaseHashKey);

            return values.entrySet()
                .stream()
                .map(entry -> new RedisLease(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        }
    }

    @Override
    public void close() {
        this.writeTimer.cancel();
        this.pool.close();
    }
}
