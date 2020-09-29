package com.github.davesters;

import com.microsoft.azure.eventprocessorhost.BaseLease;
import com.microsoft.azure.eventprocessorhost.Checkpoint;
import com.microsoft.azure.eventprocessorhost.CompleteLease;
import java.util.List;
import java.util.Map;

/**
 * A checkpoint store to house checkpoint and lease data.
 */
interface CheckpointStore extends NoExceptionAutoClosable {

    /**
     * Connect to the checkpoint store.
     */
    void connect();

    /**
     * Check if a valid connection has been made to the checkpoint store.
     * @return True if connected, false if not
     */
    boolean connected();

    /**
     * Get a checkpoint from the store.
     * @param partitionId The partition ID of the checkpoint to get
     * @return An EPH Checkpoint object
     */
    Checkpoint getCheckpoint(String partitionId);

    /**
     * Set a checkpoint in the store.
     * @param checkpoint The checkpoint to add
     */
    void setCheckpoint(Checkpoint checkpoint);

    /**
     * Delete a checkpoint from the store.
     * @param partitionId The partition id of the checkpoint to delete
     */
    void deleteCheckpoint(String partitionId);

    /**
     * Get all checkpoints from the store in a map.
     * @return A map of partition id to Checkpoint objects
     */
    Map<String, Checkpoint> getAllCheckpoints();

    /**
     * Get a lease from the store.
     * @param partitionId the partition id of the lease to get
     * @return A {@link RedisLease} object.
     */
    CompleteLease getLease(String partitionId);

    /**
     * Set a lease in the store.
     * @param lease A {@link RedisLease} object to set
     */
    void setLease(CompleteLease lease);

    /**
     * Delete a lease from the store.
     * @param partitionId The partition id of the lease to delete.
     */
    void deleteLease(String partitionId);

    /**
     * Get all leases from the store in a map.
     * @return A map of partition id to {@link RedisLease} objects
     */
    Map<String, CompleteLease> getAllLeases();

    /**
     * Get a list of leases from the store.
     * @return A list of all the leases in the form of {@link RedisLease} objects
     */
    List<BaseLease> getBaseLeases();
}
