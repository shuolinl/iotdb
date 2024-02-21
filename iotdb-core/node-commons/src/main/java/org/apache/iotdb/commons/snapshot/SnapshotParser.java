//package org.apache.iotdb.commons.snapshot;
//
//import java.nio.file.Path;
//import java.util.List;
//
///**
// * This is the interface of snapshot parser. This interface can be implemented at config and datanode
// * to handle their snapshot files.
// */
//
//public interface SnapshotParser<E> {
//
//    /**
//     * Get snapshot
//     *
//     * @return the latest snapshot Path list that will be parser.
//     */
//    public static List<Path> getSnapshotPaths();
//
//    /**
//     * Get snapshot file
//     * @param snapshotId snapshot-file id
//     * @return A file List with the snapshotID identified
//     */
//    public List<Path> getSnapshotPaths(String snapshotId);
//
//    /**
//     *  Pasrser snapshot file to statement or physicalPlan.
//     * @param snapshotFilePath snapshot-file's path. Must begin by
//     * @return Iterable statements or plans.
//     */
//    public Iterable<E> parserSnapshotFile(Path snapshotFilePath);
//}
