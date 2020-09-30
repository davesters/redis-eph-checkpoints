package com.github.davesters;

/**
 * The options object that is optionally passed into the RedisCheckpointLeaseManager constructor.
 */
public class RedisCheckpointOptions {

    private String processorHostname;
    private String redisHostname;
    private int port = 6379;
    private String password;
    private int database;
    private boolean ssl;
    private int connectTimeout = 10000;
    private int leaseDurationInMillis = 30000;
    private String checkpointKeyName;
    private boolean batchCheckpointWrites;
    private long batchIntervalInMillis = 2000;

    public RedisCheckpointOptions(String processorHostname, String redisHostname) {
        this.processorHostname = processorHostname;
        this.redisHostname = redisHostname;
    }

    /**
     * The name of the Event Processor Host name. This is the same hostname that is passed into the Event Processor
     * Host. It needs to match or else it will throw a lot of lease exceptions.
     * @return string
     */
    public String getProcessorHostname() {
        return processorHostname;
    }

    /**
     * The name of the Event Processor Host name. This is the same hostname that is passed into the Event Processor
     * Host. It needs to match or else it will throw a lot of lease exceptions.
     * @param processorHostname the EPH host name
     */
    public void setProcessorHostname(String processorHostname) {
        this.processorHostname = processorHostname;
    }

    /**
     * The host name for the Redis database.
     * @return string
     */
    public String getRedisHostname() {
        return redisHostname;
    }

    /**
     * Set the host name for the Redis database.
     * @param redisHostname Redis hostname
     */
    public void setRedisHostname(String redisHostname) {
        this.redisHostname = redisHostname;
    }

    /**
     * The port of the Redis database. Defaults to 6379.
     * @return int
     */
    public int getPort() {
        return port;
    }

    /**
     * Set the port for the Redis database. Defaults to 6379.
     * @param port The Redis port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * The Redis database password. Only required if needed to authenticate against the Redis instance.
     * @return string
     */
    public String getPassword() {
        return password;
    }

    /**
     * The Redis database password. Only required if needed to authenticate against the Redis instance.
     * @param password The password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * The Redis database number. This defaults to 0.
     * @return int
     */
    public int getDatabase() {
        return database;
    }

    /**
     * The Redis database number. This defaults to 0.
     * @param database The database number to use
     */
    public void setDatabase(int database) {
        this.database = database;
    }

    /**
     * If the connection to Redis should use SSL.
     * @return boolean
     */
    public boolean isSsl() {
        return ssl;
    }

    /**
     * If the connection to Redis should use SSL.
     * Set to true for SSL. Defaults to false.
     * @param ssl
     */
    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    /**
     * How long to wait until throwing a timeout when trying to connect to Redis.
     * Defaults to 10 seconds.
     * @return int in milliseconds
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * How long to wait until throwing a timeout when trying to connect to Redis.
     * Defaults to 10 seconds.
     * @param connectTimeout timeout length in milliseconds
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * The duration of leases for each event hub partition. This indicates how a lease should be considered valid for
     * a partition. Defaults to 30 seconds.
     * @return int in milliseconds
     */
    public int getLeaseDurationInMillis() {
        return leaseDurationInMillis;
    }

    /**
     * The duration of leases for each event hub partition. This indicates how a lease should be considered valid for
     * a partition. Defaults to 30 seconds.
     * @param leaseDurationInMillis duration in milliseconds
     */
    public void setLeaseDurationInMillis(int leaseDurationInMillis) {
        this.leaseDurationInMillis = leaseDurationInMillis;
    }

    /**
     * The name of the Redis key to use as a prefix for checkpoints and leases. This should be set to the same name
     * for any consumer group. i.e. If you have 2 hosts sharing a consumer group and partitions, they should have the
     * same key name. This will store the checkpoints in redis under this key so both consumers can share it.
     * @return string
     */
    public String getCheckpointKeyName() {
        return checkpointKeyName;
    }

    /**
     * The name of the Redis key to use as a prefix for checkpoints and leases. This should be set to the same name
     * for any consumer group. i.e. If you have 2 hosts sharing a consumer group and partitions, they should have the
     * same key name. This will store the checkpoints in redis under this key so both consumers can share it.
     * @param checkpointKeyName the key name prefix
     */
    public void setCheckpointKeyName(String checkpointKeyName) {
        this.checkpointKeyName = checkpointKeyName;
    }

    /**
     * If the checkpoint writes should be batched. This is usually good for high volume traffic so it is not
     * writing checkpoints for every event and overloading Redis. The writer will only write the latest
     * checkpoint for each partition in a batch. Defaults to false.
     * If you have a high-volume, you most likely want to set this to true.
     * @return boolean
     */
    public boolean isBatchCheckpointWrites() {
        return batchCheckpointWrites;
    }

    /**
     * If the checkpoint writes should be batched. This is usually good for high volume traffic so it is not
     * writing checkpoints for every event and overloading Redis. The writer will only write the latest
     * checkpoint for each partition in a batch. Defaults to false.
     *      * If you have a high-volume, you most likely want to set this to true.
     * @param batchCheckpointWrites true if the writer should write in batches, false otherwise.
     */
    public void setBatchCheckpointWrites(boolean batchCheckpointWrites) {
        this.batchCheckpointWrites = batchCheckpointWrites;
    }

    /**
     * How often should the writer write batches when batch writing is set to true. Defaults to 2 seconds.
     * This number should be set to the amount of data you don't mind re-processing. If the service were to
     * crash before the next batch write, the checkpoint would not be updated, so when it runs again, it will
     * pick up from the last written checkpoint.
     * @return long in milliseconds
     */
    public long getBatchIntervalInMillis() {
        return batchIntervalInMillis;
    }

    /**
     * How often should the writer write batches when batch writing is set to true. Defaults to 2 seconds.
     * This number should be set to the amount of data you don't mind re-processing. If the service were to
     * crash before the next batch write, the checkpoint would not be updated, so when it runs again, it will
     * pick up from the last written checkpoint.
     * @param batchIntervalInMillis write interval in milliseconds
     */
    public void setBatchIntervalInMillis(long batchIntervalInMillis) {
        this.batchIntervalInMillis = batchIntervalInMillis;
    }
}
