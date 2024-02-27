 package org.apache.iotdb.db.tools.schema;


 import com.sun.jdi.ArrayReference;
 import org.apache.iotdb.commons.path.PartialPath;
 import org.apache.iotdb.commons.schema.SchemaConstant;
 import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
 import org.apache.iotdb.consensus.config.RatisConfig;
 import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
 import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
 import org.apache.iotdb.db.queryengine.plan.statement.Statement;
 import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
 import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
 import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.BasicInternalMNode;
 import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.MeasurementMNode;
 import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
 import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

 import java.io.*;
 import java.nio.file.Path;
 import java.util.*;

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

        // 这里只假设我们获取了 MemTree 文件，暂时先不考虑 Tag 文件，Tag 文件的解析工作后面再加入。
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(snapshotFile))) {
            byte version = ReadWriteIOUtils.readByte(inputStream);
            // 这里需要解析为statement
            StatementGener gen = new StatementGener(inputStream);
            return new Iterable<Statement>() {
                @Override
                public Iterator<Statement> iterator() {
                    return gen;
                }
            };
        } catch (Exception e) {
            // not handle temp
            return null;
        } finally {
            //
        }

    }


    private static IMemMNode deserializeMemTree(InputStream inputStream) throws IOException{
        Deque<IMemMNode> ancestors = new ArrayDeque<>();
        Deque<Integer> restChildrenNum = new ArrayDeque<>();
        deserializeMNode(ancestors, restChildrenNum,inputStream);
        int childrenNum;
        IMemMNode root = ancestors.peek();
        while(!ancestors.isEmpty()) {
            childrenNum = restChildrenNum.pop();
            if (childrenNum == 0) {
                ancestors.pop();
            } else {
                restChildrenNum.push(childrenNum - 1);
                deserializeMNode(ancestors, restChildrenNum, inputStream);
            }
        }
        return root;
    }

    private static void deserializeMNode(Deque<IMemMNode> ancestors, Deque<Integer> restChildrenNum,
                                         InputStream inputStream) throws IOException {
        byte type = ReadWriteIOUtils.readByte(inputStream);
        IMemMNode node;
        int childrenNum;
        String name;
        long tagoffset;
        switch (type) {
            case INTERNAL_MNODE_TYPE:
            case STORAGE_GROUP_MNODE_TYPE:
                childrenNum = ReadWriteIOUtils.readInt(inputStream);
                name = ReadWriteIOUtils.readString(inputStream);
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
            case MEASUREMENT_MNODE_TYPE:
                childrenNum = 0;
                name = ReadWriteIOUtils.readString(inputStream);
                MeasurementSchema schema = MeasurementSchema.deserializeFrom(inputStream);
                String alias = ReadWriteIOUtils.readString(inputStream);
                tagoffset = ReadWriteIOUtils.readLong(inputStream);
                node = new BasicInternalMNode(null, name);
                break;

            case LOGICAL_VIEW_MNODE_TYPE:
                childrenNum = 0;
                name = ReadWriteIOUtils.readString(inputStream);
                node = new BasicInternalMNode(null, name);
                LogicalViewSchema logicalViewSchema = LogicalViewSchema.deserializeFrom(inputStream);
                tagoffset = ReadWriteIOUtils.readLong(inputStream);
                break;
            default:
                throw new IOException("Unrecognized MNode type" + type);
        }

        if (!ancestors.isEmpty()) {
            node.setParent(ancestors.peek());
            ancestors.peek().addChild(node);
        }
        if (childrenNum > 0 || isStorageGroupType(type)) {
            ancestors.push(node);
            restChildrenNum.push(childrenNum);
        }
    }

    private static IMemMNode deserializeInternalMNode(InputStream inputStream) throws IOException {
        String name = ReadWriteIOUtils.readString(inputStream);
        IMemMNode node = new BasicInternalMNode(null, name);
        ReadWriteIOUtils.readInt(inputStream);
        ReadWriteIOUtils.readBool(inputStream);
        return node;
    }

    private static class StatementGener implements Iterator<Statement> {
        private IMemMNode curNode;

        private IMemMNode root;
        private int index = 0;

        private InputStream inputStream;

        public StatementGener(InputStream inputStream) {
            this.inputStream = inputStream;
            this.curNode = new BasicInternalMNode(null, "root");
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Statement next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            index ++;
            return new AuthorStatement(AuthorType.CREATE_ROLE);
        }
    }
 }
