# Redis EPH Checkpoints

Use a Redis database for checkpoints with using the Azure Event Processor Host (EPH) for Azure Event Hubs.

## Add to your project

Add the repository URL to your `pom.xml` file.

```xml
<repository>
    <snapshots>
        <enabled>false</enabled>
    </snapshots>
    <id>bintray-davesters-java</id>
    <name>bintray</name>
    <url>https://dl.bintray.com/davesters/java</url>
</repository>
```

Add the depedency to your `pom.xml` file.

```xml
<dependency>
    <groupId>com.github.davesters</groupId>
    <artifactId>redis-eph-checkpoints</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Example Usage

```java
String processorHostName = EventProcessorHost.createHostName("hostname");
RedisCheckpointOptions options = new RedisCheckpointOptions(processorHostName, "redis.host.name");
options.setPassword("password")
options.setSsl(true);

RedisCheckpointLeaseManager checkpointLeaseManager = new RedisCheckpointLeaseManager(options);

eventProcessorHost = EventProcessorHost.EventProcessorHostBuilder
  .newBuilder(hostName, "consumer-group")
  .useUserCheckpointAndLeaseManagers(checkpointLeaseManager, checkpointLeaseManager)
  .useEventHubConnectionString("connection-string")
  .build();
```
