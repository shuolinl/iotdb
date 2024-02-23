 package org.apache.iotdb.db.tools.schema;


 import com.sun.jdi.ArrayReference;
 import org.apache.iotdb.commons.path.PartialPath;
 import org.apache.iotdb.commons.schema.SchemaConstant;
 import org.apache.iotdb.consensus.config.RatisConfig;
 import org.apache.iotdb.db.queryengine.plan.statement.Statement;
 import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
 import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.BasicInternalMNode;
 import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

 import java.io.*;
 import java.nio.file.Path;
 import java.util.ArrayDeque;
 import java.util.List;
 import java.util.Collections;
 import java.util.Deque;

 import static org.apache.iotdb.commons.schema.SchemaConstant.*;

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

        if (!snapshotFile.getName().equals(SchemaConstant.MTREE_SNAPSHOT)) {
            throw new IllegalArgumentException(String.format("%s is not allowed, only support %s", snapshotFile.getName(), SchemaConstant.MTREE_SNAPSHOT));
        }

        // 这里只假设我们获取了  MemTree 文件，暂时先不考虑 Tag 文件，Tag 文件的解析工作后面再加入。
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(snapshotFile))) {
            byte version = ReadWriteIOUtils.readByte(inputStream);







        } catch (Exception e) {
            // not handle temp.
        }
    }


    private IMemMNode deserializeMemTree(InputStream inputStream) {
        Deque<IMemMNode> ancestors = new ArrayDeque<>();
        Deque<Integer> restChildrenNum = new ArrayDeque<>();
        deserializeMNode()
    }

    private static void deserializeMNode(Deque<IMemMNode> ancestors, Deque<Integer> restChildrenNum,
                                         InputStream inputStream) throws IOException {
        byte type = ReadWriteIOUtils.readByte(inputStream);
        IMemMNode node;
        int childrenNum;
        switch (type) {
            case INTERNAL_MNODE_TYPE:
            case STORAGE_GROUP_MNODE_TYPE:
                childrenNum = ReadWriteIOUtils.readInt(inputStream);
                String name = ReadWriteIOUtils.readString(inputStream);
                node = new BasicInternalMNode(null, name);
                ReadWriteIOUtils.readInt(inputStream);
                ReadWriteIOUtils.readBool(inputStream);
                break;
            case ENTITY_MNODE_TYPE:
            case STORAGE_GROUP_ENTITY_MNODE_TYPE:
                childrenNum = ReadWriteIOUtils.readInt(inputStream);
                node  = deserializeInternalMNode(inputStream);
                int templateid = ReadWriteIOUtils.readInt(inputStream);
                boolean useTemplate = ReadWriteIOUtils.readBool(inputStream);
                boolean isAligned = ReadWriteIOUtils.readBoolObject(inputStream);
                break;





        }


    }

    private static IMemMNode deserializeInternalMNode(InputStream inputStream) throws IOException {
        String name = ReadWriteIOUtils.readString(inputStream);
        IMemMNode node = new BasicInternalMNode(null, name);
        ReadWriteIOUtils.readInt(inputStream);
        ReadWriteIOUtils.readBool(inputStream);
        return node;
    }

    private static IMemMNode deserializeStorageGroupMNode(InputStream inputStream) throws IOException {
        String name = ReadWriteIOUtils.readString(inputStream);
        IMemMNode node = new Basic
    }




 }
