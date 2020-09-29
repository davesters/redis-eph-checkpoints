package com.github.davesters;

import com.microsoft.azure.eventprocessorhost.Checkpoint;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisCheckpointLeaseManagerTest {

    @Test
    void checkpointStoreExists_returnsTrue() {
        CheckpointStore store = mock(CheckpointStore.class);
        when(store.connected()).thenReturn(true);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Boolean> response = manager.checkpointStoreExists();

        verify(store, times(1)).connected();
        response.whenComplete((result, err) -> {
            assertNull(err);
            assertTrue(result);
        });
    }

    @Test
    void createCheckpointStoreIfNotExists_callsConnectWhenNotConnected() {
        CheckpointStore store = mock(CheckpointStore.class);
        when(store.connected()).thenReturn(false);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.createCheckpointStoreIfNotExists();

        verify(store, times(1)).connected();
        verify(store, times(1)).connect();
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void createCheckpointStoreIfNotExists_doesNotCallConnectWhenConnected() {
        CheckpointStore store = mock(CheckpointStore.class);
        when(store.connected()).thenReturn(true);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.createCheckpointStoreIfNotExists();

        verify(store, times(1)).connected();
        verify(store, never()).connect();
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void deleteCheckpointStore() {
        CheckpointStore store = mock(CheckpointStore.class);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.deleteCheckpointStore();

        verify(store, times(1)).close();
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void getCheckpoint() {
        Checkpoint checkpoint = new Checkpoint("partition_id");

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getCheckpoint("partition_id")).thenReturn(checkpoint);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Checkpoint> response = manager.getCheckpoint("partition_id");

        verify(store, times(1)).getCheckpoint("partition_id");
        response.whenComplete((result, err) -> {
            assertNull(err);
            assertEquals("partition_id", result.getPartitionId());
        });
    }

    @Test
    void createAllCheckpointsIfNotExists_createsCheckpointsForMissingPartitions() {
        Map<String, Checkpoint> allCheckpoints = new HashMap<>();
        allCheckpoints.put("p1", new Checkpoint("p1"));
        allCheckpoints.put("p5", new Checkpoint("p5"));

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getAllCheckpoints()).thenReturn(allCheckpoints);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager
            .createAllCheckpointsIfNotExists(Arrays.asList("p1", "p2", "p3", "p4", "p5"));

        verify(store, times(1)).getAllCheckpoints();
        verify(store, times(3)).setCheckpoint(any(Checkpoint.class));
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void updateCheckpoint() {
    }

    @Test
    void deleteCheckpoint() {
    }

    @Test
    void getLeaseDurationInMilliseconds() {
    }

    @Test
    void leaseStoreExists() {
    }

    @Test
    void createLeaseStoreIfNotExists() {
    }

    @Test
    void deleteLeaseStore() {
    }

    @Test
    void getLease() {
    }

    @Test
    void getAllLeases() {
    }

    @Test
    void createAllLeasesIfNotExists() {
    }

    @Test
    void deleteLease() {
    }

    @Test
    void acquireLease() {
    }

    @Test
    void renewLease() {
    }

    @Test
    void releaseLease() {
    }

    @Test
    void updateLease() {
    }
}
