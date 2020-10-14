package com.github.davesters;

import com.microsoft.azure.eventprocessorhost.BaseLease;
import com.microsoft.azure.eventprocessorhost.Checkpoint;
import com.microsoft.azure.eventprocessorhost.CompleteLease;
import com.microsoft.azure.eventprocessorhost.ICheckpointManager;
import com.microsoft.azure.eventprocessorhost.ILeaseManager;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages checkpoints and leases using a Redis database.
 */
public class RedisCheckpointLeaseManager implements ICheckpointManager, ILeaseManager {

    private static final Logger LOG = LoggerFactory.getLogger(RedisCheckpointLeaseManager.class.getSimpleName());

    private final RedisCheckpointOptions options;
    private final CheckpointStore store;

    /**
     * Construct a new RedisCheckpointLeaseManager with the processor and redis hostnames.
     * All other options will be set to their default values.
     * @param processorHostName The name of the Event Processor Host name
     * @param redisHostname The hostname of the redis database
     */
    public RedisCheckpointLeaseManager(String processorHostName, String redisHostname) {
        this(new RedisCheckpointOptions(processorHostName, redisHostname));
    }

    /**
     * Construct a new RedisCheckpointLeaseManager an options object.
     * @param options The options object
     */
    public RedisCheckpointLeaseManager(RedisCheckpointOptions options) {
        this(options, new RedisCheckpointStore(options));
    }

    // This is only used in testing to pass in a mock checkpoint store.
    RedisCheckpointLeaseManager(RedisCheckpointOptions options, CheckpointStore store) {
        this.options = options;
        this.store = store;
    }

    @Override
    public CompletableFuture<Boolean> checkpointStoreExists() {
        return CompletableFuture.completedFuture(this.store.connected());
    }

    @Override
    public CompletableFuture<Void> createCheckpointStoreIfNotExists() {
        if (!this.store.connected()) {
            LOG.debug("Connecting to redis checkpoint store");
            this.store.connect();
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteCheckpointStore() {
        this.store.close();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Checkpoint> getCheckpoint(String partitionId) {
        return CompletableFuture.completedFuture(this.store.getCheckpoint(partitionId));
    }

    @Override
    public CompletableFuture<Void> createAllCheckpointsIfNotExists(List<String> partitionIds) {
        Map<String, Checkpoint> checkpoints = this.store.getAllCheckpoints();

        partitionIds.forEach(id -> {
            if (checkpoints.containsKey(id)) {
                return;
            }

            LOG.debug("creating checkpoint for partition {}", id);
            this.store.setCheckpoint(new Checkpoint(id, options.getInitialCheckpointOffset(), 0));
        });

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateCheckpoint(CompleteLease lease, Checkpoint checkpoint) {
        this.store.setCheckpoint(checkpoint);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteCheckpoint(String partitionId) {
        this.store.deleteCheckpoint(partitionId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int getLeaseDurationInMilliseconds() {
        return this.options.getLeaseDurationInMillis();
    }

    @Override
    public CompletableFuture<Boolean> leaseStoreExists() {
        return CompletableFuture.completedFuture(this.store.connected());
    }

    @Override
    public CompletableFuture<Void> createLeaseStoreIfNotExists() {
        if (!this.store.connected()) {
            LOG.debug("Connecting to redis lease store");
            this.store.connect();
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteLeaseStore() {
        this.store.close();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<CompleteLease> getLease(String partitionId) {
        CompleteLease lease = this.store.getLease(partitionId);
        return CompletableFuture.completedFuture(lease);
    }

    @Override
    public CompletableFuture<List<BaseLease>> getAllLeases() {
        return CompletableFuture.completedFuture(this.store.getBaseLeases());
    }

    @Override
    public CompletableFuture<Void> createAllLeasesIfNotExists(List<String> partitionIds) {
        Map<String, CompleteLease> leases = this.store.getAllLeases();

        partitionIds.forEach(id -> {
            if (leases.containsKey(id)) {
                return;
            }

            LOG.debug("Creating lease for partition {}", id);
            RedisLease lease = new RedisLease(id, 0, 0);
            lease.setOwner("");
            lease.setIsOwned(false);
            this.store.setLease(lease);
        });

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteLease(CompleteLease lease) {
        RedisLease leaseToDelete = (RedisLease) lease;
        RedisLease storedLease = (RedisLease) this.store.getLease(leaseToDelete.getPartitionId());

        if (hasLeaseBeenStolen(storedLease)) {
            // Don't delete the lease because another host may have claimed it.
            return CompletableFuture.completedFuture(null);
        }

        this.store.deleteLease(leaseToDelete.getPartitionId());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> acquireLease(CompleteLease lease) {
        RedisLease leaseToAcquire = (RedisLease) lease;
        RedisLease storedLease = (RedisLease) this.store.getLease(leaseToAcquire.getPartitionId());

        LOG.debug("acquiring lease for partition {}", leaseToAcquire.getPartitionId());

        // If we already own this lease and it is not expired, then don't re-acquire it
        if (storedLease.isOwnedBy(this.options.getProcessorHostname()) && !storedLease.isExpired()) {
            LOG.debug("lease already owned. skipping acquire for partition {}", leaseToAcquire.getPartitionId());
            return CompletableFuture.completedFuture(false);
        }

        // If this lease is not already owned by us, then increment the epoch because we are claiming it.
        // We only increment the epoch on first acquisition, or else it seems to breaks the event hub processor host.
        if (!storedLease.isOwnedBy(this.options.getProcessorHostname())) {
            LOG.debug("incrementing epoch for lease for partition {}", leaseToAcquire.getPartitionId());
            leaseToAcquire.incrementEpoch();
        }

        leaseToAcquire.setOwner(this.options.getProcessorHostname());
        leaseToAcquire.setIsOwned(true);
        leaseToAcquire.setExpireMillis(Instant.now().toEpochMilli() + getLeaseDurationInMilliseconds());
        this.store.setLease(leaseToAcquire);

        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> renewLease(CompleteLease lease) {
        RedisLease leaseToRenew = (RedisLease) lease;
        RedisLease storedLease = (RedisLease) this.store.getLease(leaseToRenew.getPartitionId());

        LOG.debug("renewing lease for partition {}", leaseToRenew.getPartitionId());
        if (hasLeaseBeenStolen(storedLease)) {
            LOG.debug("lease stolen. skipping renew for partition {}", leaseToRenew.getPartitionId());
            // Don't renew the lease because another may have host claimed it.
            return CompletableFuture.completedFuture(false);
        }

        // If the stored release has no owner, then most likely it has been released, so don't renew it.
        // (i.e. return false)
        if (storedLease.getOwner().trim().isEmpty()) {
            LOG.debug("lease released. skipping renew for partition {}", leaseToRenew.getPartitionId());
            return CompletableFuture.completedFuture(false);
        }

        // Update the expiry on renewal
        leaseToRenew.setExpireMillis(Instant.now().toEpochMilli() + getLeaseDurationInMilliseconds());
        this.store.setLease(leaseToRenew);

        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Void> releaseLease(CompleteLease lease) {
        LOG.debug("releasing lease for partition {}", lease.getPartitionId());

        // No need to do anything here. The old lease will expire and eventually get picked up again by a new host.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> updateLease(CompleteLease lease) {
        RedisLease leaseToUpdate = (RedisLease) lease;
        RedisLease storedLease = (RedisLease) this.store.getLease(leaseToUpdate.getPartitionId());

        LOG.debug("updating lease for partition {}", leaseToUpdate.getPartitionId());
        if (hasLeaseBeenStolen(storedLease)) {
            LOG.debug("lease stolen. skipping update for partition {}", leaseToUpdate.getPartitionId());

            // Don't update the lease because another host may have claimed it.
            return CompletableFuture.completedFuture(false);
        }

        this.store.setLease(new CompleteLease(leaseToUpdate));

        return CompletableFuture.completedFuture(true);
    }

    // Check if a lease has been stolen.
    // returns true if the owner is different than what we expect. false if the owners are the same.
    private boolean hasLeaseBeenStolen(RedisLease leaseInStore) {
        // If there is no owner set on the lease in the store, then it has not been stolen.
        if (leaseInStore.getOwner().trim().isEmpty()) {
            return false;
        }

        // If the same owner, then it has not been stolen.
        if (leaseInStore.isOwnedBy(this.options.getProcessorHostname())) {
            return false;
        }

        // If the lease in store is not expired, then another host owns it or has stolen it.
        return !leaseInStore.isExpired();
    }
}

