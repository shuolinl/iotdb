package org.apache.iotdb.db.tools.schema;

import org.apache.iotdb.tsfile.utils.Pair;

import java.nio.file.Path;

public class SchemaRegionSnapshotUnit extends Pair<Path, Path> {
  public SchemaRegionSnapshotUnit(Path path, Path path2) {
    super(path, path2);
  }
}
