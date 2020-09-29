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

    RedisCheckpointOptions(String processorHostname, String redisHostname) {
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

    public String getRedisHostname() {
        return redisHostname;
    }

    public void setRedisHostname(String redisHostname) {
        this.redisHostname = redisHostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getLeaseDurationInMillis() {
        return leaseDurationInMillis;
    }

    public void setLeaseDurationInMillis(int leaseDurationInMillis) {
        this.leaseDurationInMillis = leaseDurationInMillis;
    }

    public String getCheckpointKeyName() {
        return checkpointKeyName;
    }

    public void setCheckpointKeyName(String checkpointKeyName) {
        this.checkpointKeyName = checkpointKeyName;
    }

    public boolean isBatchCheckpointWrites() {
        return batchCheckpointWrites;
    }

    public void setBatchCheckpointWrites(boolean batchCheckpointWrites) {
        this.batchCheckpointWrites = batchCheckpointWrites;
    }

    public long getBatchIntervalInMillis() {
        return batchIntervalInMillis;
    }

    public void setBatchIntervalInMillis(long batchIntervalInMillis) {
        this.batchIntervalInMillis = batchIntervalInMillis;
    }
}
