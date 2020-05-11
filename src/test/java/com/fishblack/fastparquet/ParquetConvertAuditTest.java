package com.fishblack.fastparquet;

import com.fishblack.fastparquet.common.ConvertResult;
import com.fishblack.fastparquet.common.FieldMetadata;
import com.fishblack.fastparquet.common.ParquetConversionException;
import com.fishblack.fastparquet.reader.ParquetDataReader;
import com.fishblack.fastparquet.utils.FastParquetUtils;
import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import com.fishblack.fastparquet.utils.ParquetConverter;
import com.fishblack.fastparquet.writer.ParquetDataWriter;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ParquetConvertAuditTest {
    private static String testinputDir;
    private static final String fs = File.separator;
    private static final String tmpDir = System.getProperty("java.io.tmpdir");

    @BeforeClass
    public static void setUp() {
        testinputDir = ConfigUtils.getInstance().getResourceDir() + fs + "testdata" + fs + "parquet";
    }

    @Test
    public void testParquetExtraMetadata() throws ParquetConversionException, IOException, ParserConfigurationException, SAXException {
        String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
        try {
            Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "data_types_test_type_options.xml"));

            List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);

            Map<String, String> extraMetadata = new HashMap<>();

            ParquetConverter.canonicalToParquet(new FileInputStream(testinputDir + fs + "data_types_test_with_error.csv"), fields, false,  parquetPath, extraMetadata);

            Map<String, String> expectedMetadata = ParquetAvroUtils.getParquetKeyValueMetadata(parquetPath);

            String resultCode = expectedMetadata.get(ConvertResult.AUDIT_CONVERT_RESULT_KEY);
            String messages = expectedMetadata.get(ConvertResult.AUDIT_DETAIL_MESSAGE_KEY);
            ConvertResult result = FastParquetUtils.objectMapper.readValue(messages, ConvertResult.class);

            Assert.assertEquals(ConvertResult.Result.PARTIAL_SUCCESS.toString(), resultCode);
            Assert.assertEquals(2, result.getFailureCount());
            Assert.assertEquals(2, result.getErrors().size());
            Assert.assertTrue(result.getErrors().containsKey(11L));
            Assert.assertTrue(result.getErrors().get(11L).containsKey("f5"));
        } finally {
            FastParquetUtils.deleteWithWarning(new File(parquetPath));
        }
    }

    @Test
    public void testParquetExtraMetadataWithStreamData() throws IOException, ParquetConversionException {
        File tmpFile = File.createTempFile("tmp", ".parquet");
        tmpFile.delete();
        tmpFile.deleteOnExit();
        String[] data1 = {"2110173.5099999998"};
        String[] data2 = {"229a3517.4240000001"};
        String[] data3 = {"2110173.5099999998"};
        String[] data4 = {"2293517.4240000001"};
        String[] data5 = {"211c0173.5099999998"};
        String[] data6 = {"229d517.4240000001"};
        String[] data7 = {"2110173.5099999998"};
        String[] data8 = {"22931517.4240000001"};
        String[] data9 = {"2293d517.4240000001"};
        String[] data10 = {"2293d517.4240000001"};
        String[] data11 = {"2293d517.4240000001"};
        List<FieldMetadata> fields = new ArrayList<FieldMetadata>();
        fields.add(new FieldMetadata("FIELD1", "double"));
        ParquetDataWriter writer = new ParquetDataWriter(fields, tmpFile.getAbsolutePath());
        try {
            writer.write(data1);
            ConvertResult.InstantStatistic statistic = writer.getErrorStatistics();
            Assert.assertTrue(0 == statistic.getFailureCount());
            Assert.assertTrue(0 == statistic.getFailurePercentage());
            writer.write(data2);
            ConvertResult.InstantStatistic statistic2 = writer.getErrorStatistics();
            Assert.assertTrue(1 == statistic2.getFailureCount());
            Assert.assertTrue(50 == statistic2.getFailurePercentage());
            writer.write(data3);
            writer.write(data4);
            writer.write(data5);
            writer.write(data6);
            writer.write(data7);
            writer.write(data8);
            ConvertResult.InstantStatistic statistic3 = writer.getErrorStatistics();
            Assert.assertTrue(3 == statistic3.getFailureCount());
            Assert.assertTrue(statistic3.getFailurePercentage() <= 50);
            writer.write(data9);
            writer.write(data10);
            writer.write(data11);
        }finally {
            writer.close();
        }
        Assert.assertTrue(tmpFile.exists());
        ParquetDataReader reader = new ParquetDataReader(tmpFile.getAbsolutePath());

        try {
            Map<String, String> expectedMetadata = ParquetAvroUtils.getParquetKeyValueMetadata(tmpFile.getAbsolutePath());
            String resultCode = expectedMetadata.get(ConvertResult.AUDIT_CONVERT_RESULT_KEY);
            Assert.assertEquals(ConvertResult.Result.PARTIAL_SUCCESS.toString(), resultCode);
        } finally {
            reader.close();
        }
    }

    @Test
    public void testParquetExtraMetadataWithStreamDataWithoutAuditMetadata() throws IOException, ParquetConversionException {
        File tmpFile = File.createTempFile("tmp", ".parquet");
        tmpFile.delete();
        tmpFile.deleteOnExit();
        String[] data1 = {"88525","1330999859","P1000001","TL26119","General Electric","GE 25-Foot Phone Line Cord","7.99","30878261197","2000-01-04T00:00:00.000Z"};
        System.out.println(data1.length);

        List<FieldMetadata> fields = new ArrayList<FieldMetadata>();
        fields.add(new FieldMetadata("f0","number"));
        fields.add(new FieldMetadata("f1","number"));
        fields.add(new FieldMetadata("f2","varchar(8)"));
        fields.add(new FieldMetadata("f3","varchar(18)"));
        fields.add(new FieldMetadata("f4","varchar(29)"));
        fields.add(new FieldMetadata("f5","varchar(113)"));
        fields.add(new FieldMetadata("f6","number"));
        fields.add(new FieldMetadata("f7","number"));
        fields.add(new FieldMetadata("f8","timestamps"));

        ParquetDataWriter writer = new ParquetDataWriter(fields, tmpFile.getAbsolutePath(), new HashMap<>(), false);
        try {
            writer.write(data1);
        }finally {
            writer.close();
        }
        Assert.assertTrue(tmpFile.exists());
        ParquetDataReader reader = new ParquetDataReader(tmpFile.getAbsolutePath());

        try {
            Map<String, String> expectedMetadata = ParquetAvroUtils.getParquetKeyValueMetadata(tmpFile.getAbsolutePath());
            String resultCode = expectedMetadata.get(ConvertResult.AUDIT_CONVERT_RESULT_KEY);
            Assert.assertNull(resultCode);
        } finally {
            reader.close();
        }

    }
}
