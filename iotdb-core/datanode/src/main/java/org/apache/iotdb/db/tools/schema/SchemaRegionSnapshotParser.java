package org.apache.iotdb.db.tools.schema;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.snapshot.MemMTreeSnapshotUtil.MNodeDeserializer;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.commons.schema.SchemaConstant.ENTITY_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.LOGICAL_VIEW_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.MEASUREMENT_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_ENTITY_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.isStorageGroupType;

public class SchemaRegionSnapshotParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegionSnapshotParser.class);

  private static StatementGener statementGener;

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  public static List<SchemaRegionSnapshotUnit> getSnapshotPaths() {
    String snapshotPath = CONFIG.getSchemaRegionConsensusDir();
    File snapshotDir = new File(snapshotPath);
    ArrayList<Path> schemaRegionList = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(snapshotDir.toPath())) {
      for (Path path : stream) {
        if (path.toFile().isDirectory()) {
          schemaRegionList.add(path);
        }
      }
    } catch (IOException exception) {
      LOGGER.warn("cannot construct snapshot directory stream", exception);
      return null;
    }
    if (schemaRegionList.isEmpty()) {
      return null;
    }

    ArrayList<Path> latestSnapshots = new ArrayList<>();
    for (Path regionPath : schemaRegionList) {
      ArrayList<Path> snapshotList = new ArrayList<>();
      try (DirectoryStream<Path> stream =
          Files.newDirectoryStream(Paths.get(regionPath.toString() + File.separator + "sm"))) {
        for (Path path : stream) {
          if (path.toFile().isDirectory()) {
            snapshotList.add(path);
          }
        }
      } catch (IOException exception) {
        LOGGER.warn("cannot construct snapshot for region path {}", regionPath);
      }
      Path[] pathArray = snapshotList.toArray(new Path[0]);
      Arrays.sort(
          pathArray,
          (o1, o2) -> {
            String index1 = o1.toFile().getName().split("_")[1];
            String index2 = o2.toFile().getName().split("_")[2];
            return Long.compare(Long.parseLong(index1), Long.parseLong(index2));
          });
      if (pathArray.length != 0) {
        latestSnapshots.add(pathArray[0]);
      }
    }

    if (latestSnapshots.isEmpty()) {
      return null;
    }

    ArrayList<SchemaRegionSnapshotUnit> snapshotUnits = new ArrayList<>();
    for (Path path : latestSnapshots) {

      File mtreeSnapshot =
          SystemFileFactory.INSTANCE.getFile(path + File.separator + SchemaConstant.MTREE_SNAPSHOT);
      File tagSnapshot =
          SystemFileFactory.INSTANCE.getFile(
              path + File.separator + SchemaConstant.TAG_LOG_SNAPSHOT);
      SchemaRegionSnapshotUnit unit =
          new SchemaRegionSnapshotUnit(
              mtreeSnapshot.exists() ? mtreeSnapshot.toPath() : null,
              tagSnapshot.exists() ? tagSnapshot.toPath() : null);
      if (unit.left != null) {
        snapshotUnits.add(unit);
      }
    }
    return snapshotUnits;
  }

  public static List<SchemaRegionSnapshotUnit> getSnapshotPaths(String snapshotId) {
    String snapshotPath = CONFIG.getSchemaRegionConsensusDir();
    File snapshotDir = new File(snapshotPath);
    ArrayList<Path> schemaRegionList = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(snapshotDir.toPath())) {
      for (Path path : stream) {
        if (path.toFile().isDirectory()) {
          schemaRegionList.add(path);
        }
      }
    } catch (IOException exception) {
      LOGGER.warn("cannot construct snapshot directory stream", exception);
      return null;
    }
    if (schemaRegionList.isEmpty()) {
      return null;
    }
    ArrayList<Path> snapshotPathList = new ArrayList<>();
    for (Path path : schemaRegionList) {
      File targetPath =
          SystemFileFactory.INSTANCE.getFile(path.toString() + File.separator + "sm" + snapshotId);
      if (targetPath.exists() && targetPath.isDirectory()) {
        snapshotPathList.add(targetPath.toPath());
      }
    }
    ArrayList<SchemaRegionSnapshotUnit> snapshotUnits = new ArrayList<>();
    for (Path path : snapshotPathList) {
      File mtreeSnapshot =
          SystemFileFactory.INSTANCE.getFile(path + File.separator + SchemaConstant.MTREE_SNAPSHOT);
      File tagSnapshot =
          SystemFileFactory.INSTANCE.getFile(
              path + File.separator + SchemaConstant.TAG_LOG_SNAPSHOT);
      SchemaRegionSnapshotUnit unit =
          new SchemaRegionSnapshotUnit(
              mtreeSnapshot.exists() ? mtreeSnapshot.toPath() : null,
              tagSnapshot.exists() ? tagSnapshot.toPath() : null);
      if (unit.left != null) {
        snapshotUnits.add(unit);
      }
    }
    return snapshotUnits;
  }

  public static Iterable<Statement> translate2Statements(
      SchemaRegionSnapshotUnit snapshotUnit, PartialPath databasePath) throws IOException {
    if (snapshotUnit.left == null) {
      return null;
    }
    File mtreefile = snapshotUnit.left.toFile();
    File tagfile;
    if (snapshotUnit.right != null && snapshotUnit.right.toFile().exists()) {
      tagfile = snapshotUnit.right.toFile();
    } else {
      tagfile = null;
    }

    if (!mtreefile.exists()) {
      return null;
    }

    if (!mtreefile.getName().equals(SchemaConstant.MTREE_SNAPSHOT)) {
      throw new IllegalArgumentException(
          String.format(
              "%s is not allowed, only support %s",
              mtreefile.getName(), SchemaConstant.MTREE_SNAPSHOT));
    }
    if (tagfile != null && !tagfile.getName().equals(SchemaConstant.TAG_LOG_SNAPSHOT)) {
      throw new IllegalArgumentException(
          String.format(
              " %s is not allowed, only support %s",
              tagfile.getName(), SchemaConstant.TAG_LOG_SNAPSHOT));
    }
    statementGener = new StatementGener(mtreefile, tagfile, databasePath);
    return () -> statementGener;
  }

  public static void parserFinshWithoutExp() throws IOException {
    if (statementGener.lastExcept != null) {
      throw new IOException();
    }
  }

  private static class StatementGener implements Iterator<Statement> {
    private IMemMNode curNode;

    private Exception lastExcept = null;

    // input file stream: mtree file and tag file
    private final InputStream inputStream;

    private final FileChannel tagFileChannel;

    // help to record the state of traversing
    private final Deque<IMemMNode> ancestors = new ArrayDeque<>();
    private final Deque<Integer> restChildrenNum = new ArrayDeque<>();
    private final PartialPath databaseFullPath;

    // Iterable statements

    private final Deque<Statement> statements = new ArrayDeque<>();

    // utils

    private final MNodeTranslater translater = new MNodeTranslater();

    private final MNodeDeserializer deserializer = new MNodeDeserializer();

    public StatementGener(File mtreeFile, File tagFile, PartialPath databaseFullPath)
        throws IOException {

      this.inputStream = Files.newInputStream(mtreeFile.toPath());

      if (tagFile != null) {
        this.tagFileChannel = FileChannel.open(tagFile.toPath(), StandardOpenOption.READ);
      } else {
        this.tagFileChannel = null;
      }

      this.databaseFullPath = databaseFullPath;

      Byte version = ReadWriteIOUtils.readByte(this.inputStream);
      this.curNode =
          deserializeMNode(this.ancestors, this.restChildrenNum, deserializer, this.inputStream);
    }
    /**
     * 对于 measurement 节点，需要考虑如下场景： 1. 如果 Aligned timeseries ，那么measurement 节点无需根据节点创建， 2. 如果是普通的
     * timeseries ，需要在遍历measurement的时候构造创建节点 如果遍历到 aligned timeseries,
     * 需要保证他们的子节点不创建timeseries，并且不添加tag 如果遍历到 timeseries，
     */
    @Override
    public boolean hasNext() {
      if (!this.statements.isEmpty()) {
        return true;
      }
      while (!this.ancestors.isEmpty()) {
        int childNum = restChildrenNum.pop();
        if (childNum == 0) {
          IMemMNode node = this.ancestors.pop();
          if (node.isDevice() && node.getAsDeviceMNode().isAligned()) {
            Statement stmt =
                genAlignedTimeseriesStatement(
                    node,
                    this.databaseFullPath.getDevicePath().concatPath(node.getPartialPath()),
                    this.tagFileChannel);
            this.statements.push(stmt);
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
            this.lastExcept = ioe;
            return false;
          }
          Statement stmt =
              this.curNode.accept(
                  translater,
                  this.databaseFullPath.getDevicePath().concatPath(this.curNode.getPartialPath()));
          if (stmt != null) {
            this.statements.push(stmt);
          }
          if (!this.statements.isEmpty()) {
            return true;
          }
        }
      }
      try {
        this.inputStream.close();
        if (this.tagFileChannel != null) {
          this.tagFileChannel.close();
        }
      } catch (IOException e) {
        this.lastExcept = e;
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

    private static IMemMNode deserializeMNode(
        Deque<IMemMNode> ancestors,
        Deque<Integer> restChildrenNum,
        MNodeDeserializer deserializer,
        InputStream inputStream)
        throws IOException {
      byte type = ReadWriteIOUtils.readByte(inputStream);
      int childrenNum;
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
        IMemMNode parent = ancestors.peek();
        node.setParent(ancestors.peek());
        parent.addChild(node);
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
          // Aligned timeserie will be created when node pop.
          return StatementGener.genActivateTemplateStatement(node, path);
        }
        return null;
      }

      @Override
      public Statement visitDatabaseMNode(
          AbstractDatabaseMNode<?, ? extends IMNode<?>> node, PartialPath path) {
        if (node.isDevice()) {
          return StatementGener.genActivateTemplateStatement(node, path);
        }
        return null;
      }

      @Override
      public Statement visitMeasurementMNode(
          AbstractMeasurementMNode<?, ? extends IMNode<?>> node, PartialPath path) {
        if (node.isLogicalView() || node.getParent().getAsDeviceMNode().isAligned()) {
          return null;
        } else {
          CreateTimeSeriesStatement stmt = new CreateTimeSeriesStatement();
          stmt.setPath(path);
          stmt.setAlias(node.getAlias());
          stmt.setCompressor(node.getAsMeasurementMNode().getSchema().getCompressor());
          stmt.setDataType(node.getDataType());
          stmt.setEncoding(node.getAsMeasurementMNode().getSchema().getEncodingType());
          if (node.getOffset() != 0) {
            if (statementGener.tagFileChannel != null) {
              try {
                ByteBuffer byteBuffer =
                    ByteBuffer.allocate(COMMON_CONFIG.getTagAttributeTotalSize());
                statementGener.tagFileChannel.read(byteBuffer, node.getOffset());
                byteBuffer.flip();
                Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
                    new Pair<>(
                        ReadWriteIOUtils.readMap(byteBuffer), ReadWriteIOUtils.readMap(byteBuffer));
                stmt.setTags(tagsAndAttributes.left);
                stmt.setAttributes(tagsAndAttributes.right);
              } catch (IOException exception) {
                statementGener.lastExcept = exception;
                LOGGER.warn("error when parser tag and attributes files", exception);
              }
            } else {
              LOGGER.warn("timeserie has attributes and tags but don't find tag file");
            }
          }
          return stmt;
        }
      }
    }

    private static Statement genActivateTemplateStatement(IMNode node, PartialPath path) {
      if (node.getAsDeviceMNode().isUseTemplate()) {
        return new ActivateTemplateStatement(path);
      }
      return null;
    }

    private static Statement genAlignedTimeseriesStatement(
        IMNode node, PartialPath path, FileChannel tagFileChannel) {
      IMNodeContainer<IMemMNode> measurements = node.getAsInternalMNode().getChildren();
      if (node.getAsDeviceMNode().isAligned()) {
        CreateAlignedTimeSeriesStatement stmt = new CreateAlignedTimeSeriesStatement();
        stmt.setDevicePath(path);
        for (IMemMNode measurement : measurements.values()) {
          stmt.addMeasurement(measurement.getName());
          stmt.addDataType(measurement.getAsMeasurementMNode().getDataType());
          if (measurement.getAlias() != null) {
            stmt.addAliasList(measurement.getAlias());
          } else {
            stmt.addAliasList(null);
          }
          stmt.addEncoding(measurement.getAsMeasurementMNode().getSchema().getEncodingType());
          stmt.addCompressor(measurement.getAsMeasurementMNode().getSchema().getCompressor());
          if (measurement.getAsMeasurementMNode().getOffset() >= 0) {
            if (tagFileChannel != null) {
              try {
                ByteBuffer byteBuffer =
                    ByteBuffer.allocate(COMMON_CONFIG.getTagAttributeTotalSize());
                tagFileChannel.read(byteBuffer, measurement.getAsMeasurementMNode().getOffset());
                byteBuffer.flip();
                Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
                    new Pair<>(
                        ReadWriteIOUtils.readMap(byteBuffer), ReadWriteIOUtils.readMap(byteBuffer));
                stmt.addAttributesList(tagsAndAttributes.right);
                stmt.addTagsList(tagsAndAttributes.left);
              } catch (IOException exception) {
                LOGGER.warn(
                    "error when parse tag and attributes file of node path {}",
                    measurement.getPartialPath().toString(),
                    exception);
              }
            } else {
              LOGGER.warn("measurement has set attributes or tags, but dont find snapshot files");
            }
          }
        }
        return stmt;
      }
      return null;
    }
  }
}
