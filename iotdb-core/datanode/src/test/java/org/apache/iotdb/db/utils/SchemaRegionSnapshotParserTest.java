package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.tools.schema.SchemaRegionSnapshotParser;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class SchemaRegionSnapshotParserTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private SchemaRegionSnapshotParserTestParams rawConfig;

  protected final SchemaRegionSnapshotParserTestParams testParams;

  protected static class SchemaRegionSnapshotParserTestParams {
    private final String testModeName;
    private final String schemaRegionMode;
    private final boolean isClusterMode;

    private SchemaRegionSnapshotParserTestParams(
        String testModeName, String schemaEngineMode, boolean isClusterMode) {
      this.testModeName = testModeName;
      this.schemaRegionMode = schemaEngineMode;
      this.isClusterMode = isClusterMode;
    }

    public String getTestModeName() {
      return this.testModeName;
    }

    public String getSchemaRegionMode() {
      return this.schemaRegionMode;
    }

    public boolean getClusterMode() {
      return this.isClusterMode;
    }

    @Override
    public String toString() {
      return testModeName;
    }
  }

  private String snapshotFileName;

  @Parameterized.Parameters(name = "{0}")
  public static List<SchemaRegionSnapshotParserTestParams> getTestModes() {
    return Arrays.asList(
        new SchemaRegionSnapshotParserTestParams("MemoryMode", "Memory", true),
        new SchemaRegionSnapshotParserTestParams("PBTree", "PBTree", true));
  }

  @Before
  public void setUp() throws Exception {
    rawConfig =
        new SchemaRegionSnapshotParserTestParams(
            "Raw-Config", COMMON_CONFIG.getSchemaEngineMode(), config.isClusterMode());
    COMMON_CONFIG.setSchemaEngineMode(testParams.schemaRegionMode);
    config.setClusterMode(testParams.isClusterMode);
    SchemaEngine.getInstance().init();
    if (testParams.schemaRegionMode.equals("Memory")) {
      snapshotFileName = SchemaConstant.MTREE_SNAPSHOT;
    } else if (testParams.schemaRegionMode.equals("PBTree")) {
      snapshotFileName = SchemaConstant.PBTREE_SNAPSHOT;
    }
  }

  @After
  public void tearDown() throws Exception {
    SchemaEngine.getInstance().clear();
    cleanEnv();
    COMMON_CONFIG.setSchemaEngineMode(rawConfig.schemaRegionMode);
    config.setClusterMode(rawConfig.isClusterMode);
  }

  protected void cleanEnv() throws IOException {
    FileUtils.deleteDirectory(new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()));
  }

  public SchemaRegionSnapshotParserTest(SchemaRegionSnapshotParserTestParams params) {
    this.testParams = params;
  }

  public ISchemaRegion getSchemaRegion(String database, int schemaRegionId) throws Exception {
    SchemaRegionId regionId = new SchemaRegionId(schemaRegionId);
    if (SchemaEngine.getInstance().getSchemaRegion(regionId) == null) {
      SchemaEngine.getInstance().createSchemaRegion(new PartialPath(database), regionId);
    }
    return SchemaEngine.getInstance().getSchemaRegion(regionId);
  }

  @Test
  public void testSimpleTranslateSnapshot() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    // Tree in memtree:
    // root->sg->s1->g1->emperature
    //                |-> status
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g1.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);

    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g1.status"),
            TSDataType.BOOLEAN,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);

    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);
    File[] files = snapshotDir.listFiles();
    Assert.assertEquals(2, files.length);
    for (File file : files) {
      System.out.println(file.getName());
    }
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    File snapshot =
        SystemFileFactory.INSTANCE.getFile(
            config.getSchemaDir()
                + File.separator
                + "snapshot"
                + File.separator
                + snapshotFileName);
    System.out.println(snapshot.exists());
    Iterable<Statement> statements = SchemaRegionSnapshotParser.translate2Statements(snapshot);
    int count = 0;
    SchemaRegionSnapshotParser.parserFinshWithoutExp();
    for (Statement stmt : statements) {
      SchemaRegionSnapshotParser.parserFinshWithoutExp();
      count++;
    }
    SchemaRegionSnapshotParser.parserFinshWithoutExp();
    System.out.println(count);
  }
}
