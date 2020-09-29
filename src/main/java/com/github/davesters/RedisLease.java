package com.github.davesters;

import com.microsoft.azure.eventprocessorhost.CompleteLease;
import java.time.Instant;

/**
 * A CompleteLease object wrapped in additional context for storing them in Redis.
 */
class RedisLease extends CompleteLease {

    private long expireMillis;

    /**
     * Create a new RedisLease object.
     * @param partitionId The partition id of the lease
     * @param redisValue The value to be written to Redis
     */
    RedisLease(String partitionId, String redisValue) {
        super(partitionId);
        String[] segments = redisValue.split(",");

        this.setOwner(segments[0]);
        this.epoch = Long.parseLong(segments[1]);
        this.expireMillis = Long.parseLong(segments[2]);
        this.setIsOwned(!isExpired() && !segments[0].trim().isEmpty());
    }

    /**
     * Create a new RedisLease object.
     * @param partitionId The partition id of the lease
     * @param epoch The epoch to be written to Redis
     * @param expireMillis The expiration time in millis to be written to Redis
     */
    RedisLease(String partitionId, long epoch, long expireMillis) {
        this(partitionId, "", epoch, expireMillis);
    }

    /**
     * Create a new RedisLease object.
     * @param partitionId The partition id of the lease
     * @param owner The owner name to be written to Redis
     * @param epoch The epoch to be written to Redis
     * @param expireMillis The expiration time in millis to be written to Redis
     */
    RedisLease(String partitionId, String owner, long epoch, long expireMillis) {
        super(partitionId);

        this.setOwner(owner);
        this.epoch = epoch;
        this.expireMillis = expireMillis;
        this.setIsOwned(!isExpired() && !owner.trim().isEmpty());
    }

    public long getExpireMillis() {
        return expireMillis;
    }

    public void setExpireMillis(long expireMillis) {
        this.expireMillis = expireMillis;
    }

    public boolean isExpired() {
        return Instant.now().toEpochMilli() >= expireMillis;
    }

    /**
     * This will create a concatenation of the internal values that will then be written into Redis.
     * It will look something like this:
     * "owner,epoch,expirationMillis"
     * @return A concatenated string.
     */
    public String getRedisValue() {
        return String.format("%s,%d,%d", getOwner(), epoch, expireMillis);
    }
}
