package com.github.davesters;

import com.microsoft.azure.eventprocessorhost.BaseLease;
import com.microsoft.azure.eventprocessorhost.Checkpoint;
import com.microsoft.azure.eventprocessorhost.CompleteLease;
import java.time.Instant;
import java.util.*;
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
    void createAllCheckpointsIfNotExists_createsNoCheckpointsWhenAllExist() {
        Map<String, Checkpoint> allCheckpoints = new HashMap<>();
        allCheckpoints.put("p1", new Checkpoint("p1"));
        allCheckpoints.put("p2", new Checkpoint("p2"));
        allCheckpoints.put("p3", new Checkpoint("p3"));
        allCheckpoints.put("p4", new Checkpoint("p4"));
        allCheckpoints.put("p5", new Checkpoint("p5"));

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getAllCheckpoints()).thenReturn(allCheckpoints);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager
            .createAllCheckpointsIfNotExists(Arrays.asList("p1", "p2", "p3", "p4", "p5"));

        verify(store, times(1)).getAllCheckpoints();
        verify(store, never()).setCheckpoint(any(Checkpoint.class));
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void updateCheckpoint() {
        CheckpointStore store = mock(CheckpointStore.class);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.updateCheckpoint(null, null);

        verify(store, times(1)).setCheckpoint(isNull());
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void deleteCheckpoint() {
        CheckpointStore store = mock(CheckpointStore.class);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.deleteCheckpoint("partition_id");

        verify(store, times(1)).deleteCheckpoint("partition_id");
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void getLeaseDurationInMilliseconds_returnsDefaultDuration() {
        final int expectedDuration = 30000;
        RedisCheckpointOptions options = new RedisCheckpointOptions("", "");

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(options, null);
        int response = manager.getLeaseDurationInMilliseconds();

        assertEquals(expectedDuration, response);
    }

    @Test
    void getLeaseDurationInMilliseconds_returnsCustomDuration() {
        final int expectedDuration = 666;
        RedisCheckpointOptions options = new RedisCheckpointOptions("", "");
        options.setLeaseDurationInMillis(expectedDuration);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(options, null);
        int response = manager.getLeaseDurationInMilliseconds();

        assertEquals(expectedDuration, response);
    }

    @Test
    void leaseStoreExists() {
        CheckpointStore store = mock(CheckpointStore.class);
        when(store.connected()).thenReturn(true);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Boolean> response = manager.leaseStoreExists();

        verify(store, times(1)).connected();
        response.whenComplete((result, err) -> {
            assertNull(err);
            assertTrue(result);
        });
    }

    @Test
    void createLeaseStoreIfNotExists_callsConnectWhenNotConnected() {
        CheckpointStore store = mock(CheckpointStore.class);
        when(store.connected()).thenReturn(false);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.createLeaseStoreIfNotExists();

        verify(store, times(1)).connected();
        verify(store, times(1)).connect();
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void createLeaseStoreIfNotExists_doesNotCallConnectWhenConnected() {
        CheckpointStore store = mock(CheckpointStore.class);
        when(store.connected()).thenReturn(true);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.createLeaseStoreIfNotExists();

        verify(store, times(1)).connected();
        verify(store, never()).connect();
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void deleteLeaseStore() {
        CheckpointStore store = mock(CheckpointStore.class);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.deleteLeaseStore();

        verify(store, times(1)).close();
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void getLease() {
        CompleteLease lease = new CompleteLease("partition_id");

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getLease("partition_id")).thenReturn(lease);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<CompleteLease> response = manager.getLease("partition_id");

        verify(store, times(1)).getLease("partition_id");
        response.whenComplete((result, err) -> {
            assertNull(err);
            assertEquals("partition_id", result.getPartitionId());
        });
    }

    @Test
    void getAllLeases() {
        CompleteLease lease = new CompleteLease("partition_id");

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getBaseLeases()).thenReturn(Collections.singletonList(lease));

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<List<BaseLease>> response = manager.getAllLeases();

        verify(store, times(1)).getBaseLeases();
        response.whenComplete((result, err) -> {
            assertNull(err);
            assertEquals(1, result.size());
            assertEquals("partition_id", result.get(0).getPartitionId());
        });
    }

    @Test
    void createAllLeasesIfNotExists_createsLeasesForMissingPartitions() {
        Map<String, CompleteLease> allLeases = new HashMap<>();
        allLeases.put("p1", new CompleteLease("p1"));
        allLeases.put("p5", new CompleteLease("p5"));

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getAllLeases()).thenReturn(allLeases);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager
            .createAllLeasesIfNotExists(Arrays.asList("p1", "p2", "p3", "p4", "p5"));

        verify(store, times(1)).getAllLeases();
        verify(store, times(3)).setLease(any(CompleteLease.class));
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void createAllLeasesIfNotExists_createsNoLeasesWhenAllExist() {
        Map<String, CompleteLease> allLeases = new HashMap<>();
        allLeases.put("p1", new CompleteLease("p1"));
        allLeases.put("p2", new CompleteLease("p2"));
        allLeases.put("p3", new CompleteLease("p3"));
        allLeases.put("p4", new CompleteLease("p4"));
        allLeases.put("p5", new CompleteLease("p5"));

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getAllLeases()).thenReturn(allLeases);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager
            .createAllLeasesIfNotExists(Arrays.asList("p1", "p2", "p3", "p4", "p5"));

        verify(store, times(1)).getAllLeases();
        verify(store, never()).setLease(any(CompleteLease.class));
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void deleteLease_deletesWhenNotStolen() {
        RedisLease leaseInStore = new RedisLease("partition_id", "", 0, 0);
        RedisLease leaseToDelete = new RedisLease("partition_id", "owned", 0, 0);

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getLease("partition_id")).thenReturn(leaseInStore);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Void> response = manager.deleteLease(leaseToDelete);

        verify(store, times(1)).getLease("partition_id");
        verify(store, times(1)).deleteLease("partition_id");
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void deleteLease_doesNotDeleteWhenStolen() {
        RedisCheckpointOptions options = new RedisCheckpointOptions("owned", "");
        long twoSeconds = Instant.now().plusSeconds(2).toEpochMilli();
        RedisLease leaseInStore = new RedisLease("partition_id", "otherOwner", 0, twoSeconds);
        RedisLease leaseToDelete = new RedisLease("partition_id", "owned", 0, 0);

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getLease("partition_id")).thenReturn(leaseInStore);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(options, store);
        CompletableFuture<Void> response = manager.deleteLease(leaseToDelete);

        verify(store, times(1)).getLease("partition_id");
        verify(store, never()).deleteLease(anyString());
        response.whenComplete((result, err) -> assertNull(err));
    }

    @Test
    void updateLease_updatesWhenNotStolen() {
        RedisLease leaseInStore = new RedisLease("partition_id", "", 0, 0);
        RedisLease leaseToUpdate = new RedisLease("partition_id", "owned", 0, 0);

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getLease("partition_id")).thenReturn(leaseInStore);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(null, store);
        CompletableFuture<Boolean> response = manager.updateLease(leaseToUpdate);

        verify(store, times(1)).getLease("partition_id");
        verify(store, times(1)).setLease(any());
        response.whenComplete((result, err) -> {
            assertNull(err);
            assertTrue(result);
        });
    }

    @Test
    void updateLease_doesNotUpdateWhenStolen() {
        RedisCheckpointOptions options = new RedisCheckpointOptions("owned", "");
        long twoSeconds = Instant.now().plusSeconds(2).toEpochMilli();
        RedisLease leaseInStore = new RedisLease("partition_id", "otherOwner", 0, twoSeconds);
        RedisLease leaseToUpdate = new RedisLease("partition_id", "owned", 0, 0);

        CheckpointStore store = mock(CheckpointStore.class);
        when(store.getLease("partition_id")).thenReturn(leaseInStore);

        RedisCheckpointLeaseManager manager = new RedisCheckpointLeaseManager(options, store);
        CompletableFuture<Boolean> response = manager.updateLease(leaseToUpdate);

        verify(store, times(1)).getLease("partition_id");
        verify(store, never()).setLease(any());
        response.whenComplete((result, err) -> {
            assertNull(err);
            assertFalse(result);
        });
    }
}
