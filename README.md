# Redis EPH Checkpoints

[ ![Download](https://api.bintray.com/packages/davesters/java/redis-eph-checkpoints/images/download.svg) ](https://bintray.com/davesters/java/redis-eph-checkpoints/_latestVersion)

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
    <version>[LATEST VERSION]</version>
</dependency>
```

## Example Usage

```java
MetadataEventProcessorFactory eventProcessorFactory = context.createBean(YourProcessorFactoryClass.class);

String processorHostName = EventProcessorHost.createHostName("hostname");
RedisCheckpointOptions options = new RedisCheckpointOptions(processorHostName, "redis.host.name");
options.setCheckpointKeyName(processorHostName); // Required option
options.setLeaseDurationInMillis(45 * 1000); // Set to 45 seconds if you want. Defaults to 30 seconds.
options.setPassword("password");
options.setSsl(true);

RedisCheckpointLeaseManager checkpointLeaseManager = new RedisCheckpointLeaseManager(options);

eventProcessorHost = EventProcessorHost.EventProcessorHostBuilder
  .newBuilder(hostName, "consumer-group")
  .useUserCheckpointAndLeaseManagers(checkpointLeaseManager, checkpointLeaseManager)
  .useEventHubConnectionString("connection-string")
  .build();

PartitionManagerOptions partitionManagerOptions = new PartitionManagerOptions();
partitionManagerOptions.setLeaseDurationInSeconds(45); // Maybe want to set this to match the lease duration set above
partitionManagerOptions.setLeaseRenewIntervalInSeconds(30); // Probably want to set this to a number below the lease duration
eventProcessorHost.setPartitionManagerOptions(partitionManagerOptions);

eventProcessorHost.registerEventProcessorFactory(eventProcessorFactory, options).get();
```

Be sure to look at the `RedisCheckpointOptions` class for other available options and their descriptions.
