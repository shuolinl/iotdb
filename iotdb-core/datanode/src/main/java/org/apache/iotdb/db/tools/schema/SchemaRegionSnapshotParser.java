package org.apache.iotdb.db.tools.schema;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.impl.BasicInternalMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.snapshot.MemMTreeSnapshotUtil.MNodeDeserializer;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

import static org.apache.iotdb.commons.schema.SchemaConstant.*;

public class SchemaRegionSnapshotParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegionSnapshotParser.class);

  private static StatementGener gener;

  private static final IMNodeFactory<IMemMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory();;

  public static List<Path> getSnapshotPaths() {
    return Collections.emptyList();
  }

  public static List<Path> getSnapshotPaths(String snapshotId) {
    return Collections.emptyList();
  }

  public static Iterable<Statement> translate2Statements(File snapshotFile) throws IOException {
    if (!snapshotFile.exists()) {
      return null;
    }

    if (!snapshotFile.getName().equals(SchemaConstant.MTREE_SNAPSHOT)) {
      throw new IllegalArgumentException(
          String.format(
              "%s is not allowed, only support %s",
              snapshotFile.getName(), SchemaConstant.MTREE_SNAPSHOT));
    }
    gener = new StatementGener(snapshotFile);
    return () -> gener;
  }

  public static void parserFinshWithoutExp() throws IOException {
    if (gener.lastExcep != null) {
      throw new IOException();
    }
  }

  private static class StatementGener implements Iterator<Statement> {
    private IMemMNode curNode;

    private int childNum = 0;

    private IMemMNode root;

    private Exception lastExcep = null;

    private InputStream inputStream;

    // 用这个来标记 Aligned timeseries 的 Chlid 都在这里处理完毕了。
    private static final String MAGICAL_STR = "&^%$)";

    // 帮助记录遍历进度
    private Deque<IMemMNode> ancestors = new ArrayDeque<>();
    private Deque<Integer> restChildrenNum = new ArrayDeque<>();

    private Deque<Statement> statements = new ArrayDeque<>();

    private MNodeTranslater translater = new MNodeTranslater();

    private MNodeDeserializer deserializer = new MNodeDeserializer();

    public StatementGener(File snapshotFile) throws IOException {

      this.inputStream = new FileInputStream(snapshotFile);
      Byte version = ReadWriteIOUtils.readByte(this.inputStream);
      this.root =
          deserializeMNode(this.ancestors, this.restChildrenNum, deserializer, this.inputStream);
      this.curNode = this.root;
    }

    @Override
    public boolean hasNext() {
      while (!this.ancestors.isEmpty()) {
        this.childNum = restChildrenNum.pop();
        if (this.childNum == 0) {
          ancestors.pop();
          Statement stmt = this.curNode.accept(translater, this.curNode.getPartialPath());
          while (stmt != null) {
            this.statements.push(stmt);
            stmt = this.curNode.accept(translater, this.curNode.getPartialPath());
          }
          if (!this.statements.isEmpty()) {
            return true;
          }
        } else {
          restChildrenNum.push(childNum - 1);
          try {
            curNode =
                deserializeMNode(this.ancestors, this.restChildrenNum, deserializer, inputStream);
          } catch (IOException ioe) {
            try {
              this.inputStream.close();
            } catch (IOException e) {
              // same ioexception;
            }
            this.lastExcep = ioe;
            return false;
          }
        }
      }
      try {
        this.inputStream.close();
      } catch (IOException e) {
        this.lastExcep = e;
      }

      return false;
    }

    @Override
    public Statement next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return this.statements.pop();
    }
  }

  private static IMemMNode deserializeMNode(
      Deque<IMemMNode> ancestors,
      Deque<Integer> restChildrenNum,
      MNodeDeserializer deserializer,
      InputStream inputStream)
      throws IOException {
    byte type = ReadWriteIOUtils.readByte(inputStream);
    int childrenNum;
    String name;
    IMemMNode node;
    switch (type) {
      case INTERNAL_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeInternalMNode(inputStream);
        break;
      case STORAGE_GROUP_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeStorageGroupMNode(inputStream);
        break;
      case ENTITY_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeEntityMNode(inputStream);
        break;
      case STORAGE_GROUP_ENTITY_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeStorageGroupEntityMNode(inputStream);
        break;
      case MEASUREMENT_MNODE_TYPE:
        childrenNum = 0;
        node = deserializer.deserializeMeasurementMNode(inputStream);
        break;
      case LOGICAL_VIEW_MNODE_TYPE:
        childrenNum = 0;
        node = deserializer.deserializeLogicalViewMNode(inputStream);
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
    return node;
  }

  private static class MNodeTranslater extends MNodeVisitor<Statement, PartialPath> {

    @Override
    public Statement visitBasicMNode(IMNode<?> node, PartialPath path) {
      if (node.isDevice()) {
        Statement stmt = genActivateTemplateStatement(node);
        return stmt == null ? genAlignedTimeseriesStatement(node) : null;
      }
      return null;
    }

    @Override
    public Statement visitDatabaseMNode(
        AbstractDatabaseMNode<?, ? extends IMNode<?>> node, PartialPath path) {
      if (node.isDevice()) {
        Statement stmt = genActivateTemplateStatement(node);
        return stmt == null ? genAlignedTimeseriesStatement(node) : null;
      }
      return null;
    }

    @Override
    public Statement visitMeasurementMNode(
        AbstractMeasurementMNode<?, ? extends IMNode<?>> node, PartialPath path) {
      if (node.isLogicalView()) {
        return null;
      } else {
        CreateTimeSeriesStatement stmt = new CreateTimeSeriesStatement();
        stmt.setPath(node.getPartialPath());
        stmt.setAlias(node.getAlias());
        stmt.setCompressor(node.getAsMeasurementMNode().getSchema().getCompressor());
        stmt.setDataType(node.getDataType());
        stmt.setEncoding(node.getAsMeasurementMNode().getSchema().getEncodingType());
        return stmt;
      }
    }

    private Statement genActivateTemplateStatement(IMNode node) {
      if (node.getAsDeviceMNode().isUseTemplate()) {
        node.getAsDeviceMNode().setUseTemplate(false);
        return new ActivateTemplateStatement(node.getPartialPath());
      }
      return null;
    }

    private Statement genAlignedTimeseriesStatement(IMNode node) {
      IMNodeContainer<BasicInternalMNode> measurements =
          (IMNodeContainer<BasicInternalMNode>) node.getAsMNode().getChildren();
      if (node.getAsDeviceMNode().isAligned()) {
        CreateAlignedTimeSeriesStatement stmt = new CreateAlignedTimeSeriesStatement();
        for (BasicInternalMNode measurement : measurements.values()) {
          stmt.addMeasurement(measurement.getName());
          stmt.addDataType(measurement.getAsMeasurementMNode().getDataType());
          stmt.addAliasList(measurement.getAlias());
          stmt.addEncoding(measurement.getAsMeasurementMNode().getSchema().getTimeTSEncoding());
          stmt.addCompressor(measurement.getAsMeasurementMNode().getSchema().getCompressor());
          measurement.setName(StatementGener.MAGICAL_STR);
        }
        node.getChildren().clear();
        return stmt;
      }
      return null;
    }
  }
}
