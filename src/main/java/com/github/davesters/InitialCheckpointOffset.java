package com.github.davesters;

/**
 * Constants to specify where to start in the Event Hub when no checkpoint exists yet.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public final class InitialCheckpointOffset {
    /**
     * Start at the end of the event hub stream. i.e. don't read old data.
     */
    public static final String END_OF_STREAM = "@latest";

    /**
     * Start at the beginning of the event hub stream. i.e. read all the old data in the event hub first.
     */
    public static final String START_OF_STREAM = "-1";
}
