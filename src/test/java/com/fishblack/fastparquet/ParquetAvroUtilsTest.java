package com.fishblack.fastparquet;

import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class ParquetAvroUtilsTest {
    private static final String DATA_WITH_COMMA = "12,345,678,900.001";
    private static final String DATA_WITH_MANY_COMMA = ",,1,2,3,4,5,6,7,8,9,00.,001,";
    private static final String DATA_WITHOUT_COMMA = "12345678900.001";
    private static final String EXPECT_DATA_WITHOUT_COMMA = "12345678900.001";
    private static final String EMPTY_STRING = "";
    private static final String DATETIME_WITH_QUOTATION = "''2018-08-08 20:01:01.000''";
    private static final String EXPECT_DATETIME_WITHOUT_QUOTATION = "2018-08-08 20:01:01.000";
    private static final String DATETIME_WITH_MORE_QUOTATION = "'2018-08-08' '20:01:01.000'";
    private static final String EXPECT_DATETIME_WITH_MORE_QUOTATION = "2018-08-08' '20:01:01.000";

    @Test
    public void testRemoveComma(){
        String result = ParquetAvroUtils.removeComma(DATA_WITH_COMMA);
        Assert.assertEquals(EXPECT_DATA_WITHOUT_COMMA, result);

        result = ParquetAvroUtils.removeComma(DATA_WITHOUT_COMMA);
        Assert.assertEquals(EXPECT_DATA_WITHOUT_COMMA, result);

        result = ParquetAvroUtils.removeComma(DATA_WITH_MANY_COMMA);
        Assert.assertEquals(EXPECT_DATA_WITHOUT_COMMA, result);

        result = ParquetAvroUtils.removeComma(EMPTY_STRING);
        Assert.assertEquals(EMPTY_STRING, result);

        result = ParquetAvroUtils.removeComma(null);
        Assert.assertNull(result);
    }

    @Test
    public void testRemoveStartEndSingleQuotation(){
        String result = ParquetAvroUtils.removeStartEndSingleQuotation(DATETIME_WITH_QUOTATION);
        Assert.assertEquals(EXPECT_DATETIME_WITHOUT_QUOTATION, result);

        result = ParquetAvroUtils.removeStartEndSingleQuotation(DATETIME_WITH_MORE_QUOTATION);
        Assert.assertEquals(EXPECT_DATETIME_WITH_MORE_QUOTATION, result);

        result = ParquetAvroUtils.removeStartEndSingleQuotation(null);
        Assert.assertNull(result);
    }

    @Test
    public void testToAvroFieldValue(){
        Schema schema = Schema.create(Schema.Type.STRING);
        Object result = ParquetAvroUtils.toAvroFieldValue(schema, EMPTY_STRING);
        Assert.assertNull(result);
    }
}
