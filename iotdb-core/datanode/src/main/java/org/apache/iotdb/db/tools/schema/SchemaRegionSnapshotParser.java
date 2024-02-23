 package org.apache.iotdb.db.tools.schema;


 import org.apache.iotdb.commons.path.PartialPath;
 import org.apache.iotdb.db.queryengine.plan.statement.Statement;

 import java.io.File;
 import java.nio.file.Files;
 import java.nio.file.Path;
 import java.util.List;
 import java.util.Collections;

 public class SchemaRegionSnapshotParser  {
     /**
      * 1. 检查系统存储以及使用的共识协议，确认snapshot 生成的位置
      * 2. 获取最新的snapshot 文件
      */
    public static List<Path> getSnapshotPaths() {
        return Collections.emptyList();
    }

     /**
      *  1. 检查系统使用的存储以及使用的共识协议，确认snapshot 生成的根路径
      *  2. 在路径下查找 满足 snapshotId 的文件夹，并且按照顺序返回。
      * @param snapshotId
      * @return
      */
    public static List<Path> getSnapshotPaths(String snapshotId) {
        return Collections.emptyList();
    }

    public static Iterable<Statement> translate2Statements(File snapshotFile) {
        if (!snapshotFile.exists()) {
            return null;
        }
    }

    private class StatementIterator implements java.util.Iterator<Statement> {
        public boolean hasNext() {
            return false;
        }

        public Statement next() {
            return new Statement() {
                @Override
                public List<PartialPath> getPaths() {
                    return null;
                }
            };
        }
    }

 }
