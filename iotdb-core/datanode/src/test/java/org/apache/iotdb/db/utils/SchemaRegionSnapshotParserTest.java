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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.schemaengine.template.Template;
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
import java.util.*;

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
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    // Tree in memtree:
    // root->sg->s1->g1->temp
    //          |     |->status
    //          |->s2->g2->t2->temp
    //              |->g4->status
    //              |->g5->level
    HashMap<String, ICreateTimeSeriesPlan> planMap = new HashMap<>();
    planMap.put(
        "root.sg.s1.g1.temp",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g1.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg.s1.g1.status",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g1.status"),
            TSDataType.INT64,
            TSEncoding.TS_2DIFF,
            CompressionType.LZ4,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg.s2.g2.t2.temp",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s2.g2.t2.temp"),
            TSDataType.DOUBLE,
            TSEncoding.RLE,
            CompressionType.GZIP,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg.s2.g4.status",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s2.g4.status"),
            TSDataType.INT64,
            TSEncoding.RLE,
            CompressionType.ZSTD,
            null,
            null,
            null,
            null));
    planMap.put(
        "root.sg.s2.g5.level",
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s2.g5.level"),
            TSDataType.INT32,
            TSEncoding.GORILLA,
            CompressionType.LZMA2,
            null,
            null,
            null,
            null));
    for (ICreateTimeSeriesPlan plan : planMap.values()) {
      schemaRegion.createTimeseries(plan, 0);
    }

    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);

    File snapshot =
        SystemFileFactory.INSTANCE.getFile(
            config.getSchemaDir()
                + File.separator
                + "snapshot"
                + File.separator
                + snapshotFileName);
    Iterable<Statement> statements = SchemaRegionSnapshotParser.translate2Statements(snapshot);
    int count = 0;
    SchemaRegionSnapshotParser.parserFinshWithoutExp();
    assert statements != null;
    for (Statement stmt : statements) {
      SchemaRegionSnapshotParser.parserFinshWithoutExp();
      count++;
      CreateTimeSeriesStatement createTimeSeriesStatement = (CreateTimeSeriesStatement) stmt;
      ICreateTimeSeriesPlan plan =
          planMap.get(createTimeSeriesStatement.getPaths().get(0).toString());
      Assert.assertEquals(plan.getEncoding(), createTimeSeriesStatement.getEncoding());
      Assert.assertEquals(plan.getCompressor(), createTimeSeriesStatement.getCompressor());
      Assert.assertEquals(plan.getDataType(), createTimeSeriesStatement.getDataType());
      Assert.assertEquals(plan.getAlias(), createTimeSeriesStatement.getAlias());
      Assert.assertEquals(plan.getProps(), createTimeSeriesStatement.getProps());
      Assert.assertEquals(plan.getAttributes(), createTimeSeriesStatement.getAttributes());
      Assert.assertEquals(plan.getTags(), createTimeSeriesStatement.getTags());
    }
    SchemaRegionSnapshotParser.parserFinshWithoutExp();
    Assert.assertEquals(5, count);
    System.out.println(count);
  }

  @Test
  public void testAlignedTimeseriesTranslateSnapshot() throws Exception {
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createAlignedTimeSeries(
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg.t1.t2"),
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT32, TSDataType.INT64),
            Arrays.asList(TSEncoding.PLAIN, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.LZ4),
            Arrays.asList("alias1", "alias2"),
            Arrays.asList(
                new HashMap<String, String>() {
                  {
                    put("tag1", "t1");
                  }
                },
                new HashMap<String, String>() {
                  {
                    put("tag2", "t2");
                  }
                }),
            Arrays.asList(
                new HashMap<String, String>() {
                  {
                    put("attr1", "a1");
                  }
                },
                new HashMap<String, String>() {
                  {
                    put("attr2", "a2");
                  }
                })));
    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);
    File snapshot =
        SystemFileFactory.INSTANCE.getFile(
            config.getSchemaDir()
                + File.separator
                + "snapshot"
                + File.separator
                + snapshotFileName);
    Iterable<Statement> statements = SchemaRegionSnapshotParser.translate2Statements(snapshot);
    int count = 0;
    SchemaRegionSnapshotParser.parserFinshWithoutExp();
    assert statements != null;
    for (Statement stmt : statements) {
      count++;
    }
    Assert.assertEquals(1, count);
  }

  @Test
  public void testTemplateActivateTranslateSnapshot() throws Exception {
    if (testParams.testModeName.equals("PBTree")) {
      return;
    }
    ISchemaRegion schemaRegion = getSchemaRegion("root.sg", 0);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g1.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s1.g3.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.sg.s2.g1.temp"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        0);
    Template template =
        new Template(
            "t1",
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList(TSEncoding.PLAIN),
            Collections.singletonList(CompressionType.GZIP));
    template.setId(0);
    HashMap<String, IActivateTemplateInClusterPlan> planMap = new HashMap<>();
    IActivateTemplateInClusterPlan plan1 =
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.s2"), 1, template.getId());
    IActivateTemplateInClusterPlan plan2 =
        SchemaRegionWritePlanFactory.getActivateTemplateInClusterPlan(
            new PartialPath("root.sg.s3"), 1, template.getId());
    planMap.put("root.sg.s2", plan1);
    planMap.put("root.sg.s3", plan2);
    schemaRegion.activateSchemaTemplate(plan1, template);
    schemaRegion.activateSchemaTemplate(plan2, template);
    File snapshotDir = new File(config.getSchemaDir() + File.separator + "snapshot");
    snapshotDir.mkdir();
    schemaRegion.createSnapshot(snapshotDir);
    File snapshot =
        SystemFileFactory.INSTANCE.getFile(
            config.getSchemaDir()
                + File.separator
                + "snapshot"
                + File.separator
                + snapshotFileName);
    Iterable<Statement> statements = SchemaRegionSnapshotParser.translate2Statements(snapshot);
    int count = 0;
    SchemaRegionSnapshotParser.parserFinshWithoutExp();
    assert statements != null;
    for (Statement stmt : statements) {
      if (stmt instanceof ActivateTemplateStatement) {
        ActivateTemplateStatement ATStatement = (ActivateTemplateStatement) stmt;
        IActivateTemplateInClusterPlan plan = planMap.get(ATStatement.getPath().toString());
        Assert.assertEquals(plan.getActivatePath());
      }
    }
    Assert.assertEquals(2, count);
  }
}
