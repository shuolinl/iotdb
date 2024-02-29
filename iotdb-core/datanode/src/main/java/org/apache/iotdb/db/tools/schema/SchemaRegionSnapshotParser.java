 package org.apache.iotdb.db.tools.schema;

 import org.apache.iotdb.commons.path.PartialPath;
 import org.apache.iotdb.commons.schema.SchemaConstant;
 import org.apache.iotdb.commons.schema.node.IMNode;
 import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
 import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
 import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
 import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
 import org.apache.iotdb.db.queryengine.plan.statement.Statement;
 import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
 import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
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

     private static class StatementGener implements Iterator<Statement> {
         private IMemMNode curNode;

         private int childNum = 0;

         private IMemMNode root;
         private int index = 0;

         private Exception lastExcep = null;

         private InputStream inputStream;

         // 帮助记录遍历进度
         private Deque<IMemMNode> ancestors = new ArrayDeque<>();
         private Deque<Integer> restChildrenNum = new ArrayDeque<>();

         public StatementGener(InputStream inputStream) throws IOException {
             this.inputStream = inputStream;
             this.root = deserializeMNode(this.ancestors, this.restChildrenNum, this.inputStream);
             this.curNode = this.root;
             byte version = ReadWriteIOUtils.readByte(this.inputStream);
         }

         // hasNext 用来遍历到可供输出 statement 的节点，
         // 1. device node。用来创建timeseries， 但是创建时，需要获得measurement，因此device node 需要将它们的子节点遍历出来
         // next 用来将对应的节点转换为对应的 statement
         @Override
         public boolean hasNext() {
             while (!this.ancestors.isEmpty()) {
                 this.childNum = restChildrenNum.pop();
                 if (this.childNum == 0) {
                     // 仅当所有的子节点都遍历出来之后，才允许返回【还需要有函数判断这个节点是不是需要进行转译的】
                     // 此时，返回的便是可以进行statement 转化的节点了
                     // 需要处理这几种节点：
                     // 1. 中间节点，包含template的激活信息
                     // 2. device 节点， 包含创建信息（是否为aligned序列，需要获取其所有的子节点才可以）
                     // 3. measurement 节点，
                     ancestors.pop();
                     return true;
                 } else {
                     restChildrenNum.push(childNum - 1);
                      try {
                          curNode = deserializeMNode(this.ancestors, this.restChildrenNum, this.inputStream);
                      } catch (IOException ioe)  {
                          this.lastExcep = ioe;
                          return false;
                      }
                 }
                 if (this.curNode != null) {
                     return true;
                 } else {
                     return false;
                 }
             }
             return false;
         }

         @Override
         public Statement next() {
             if (!hasNext()) {
                 throw new NoSuchElementException();
             }
             index ++;
             return new AuthorStatement(AuthorType.CREATE_ROLE);
         }

         private Statement translateMNode(IMemMNode node) {

         }





     }

    private static IMemMNode deserializeMNode(Deque<IMemMNode> ancestors, Deque<Integer> restChildrenNum,
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

    private static class MNodeTranslater extends MNodeVisitor<Statement, PartialPath> {

        @Override
        public Statement visitBasicMNode(IMNode<?> node, PartialPath path) {
            if (node.isDevice()) {
                if (node.getAsDeviceMNode().isUseTemplate()) {
                    // 这里会带来性能下降
                    node.getAsDeviceMNode().setUseTemplate(false);
                    return new ActivateTemplateStatement(path);
                }
                IMNodeContainer<BasicInternalMNode> measurements = (IMNodeContainer<BasicInternalMNode>) node.getChildren();
                if (node.getAsDeviceMNode().isAligned()) {
                    CreateAlignedTimeSeriesStatement stmt = new CreateAlignedTimeSeriesStatement();
                    for (BasicInternalMNode measurement : measurements.values()) {
                        stmt.addMeasurement(measurement.getName());
                        stmt.addDataType(measurement.getAsMeasurementMNode().getDataType());
                        stmt.addAliasList(measurement.getAlias());
                        stmt.addEncoding(measurement.getAsMeasurementMNode().getSchema().getTimeTSEncoding());
                        stmt.addCompressor(measurement.getAsMeasurementMNode().getSchema().getCompressor());
                    }
                    return stmt;
                }
                node.ge

                } else {

                }
            }

        }
    }





 }
