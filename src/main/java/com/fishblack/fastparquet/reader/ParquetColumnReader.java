package com.fishblack.fastparquet.reader;

import java.io.Closeable;
import java.io.IOException;

/**
 * A column level reader for parquet files.
 */
public interface ParquetColumnReader extends Closeable{

    /**
     * Return true if there are still remaining data to read.
     * @return
     */
    boolean hasNext();

    /**
     * Returns the next value of current column. 
     * The returned type based on OAC field metadata, reference the mapping below:
     * <table border="1">
     * <tr><td> OAC Type </td> <td> Return Type</td></tr>
     * <tr><td> varchar </td> <td> String </td></tr>
     * <tr><td> double </td> <td> Double</td></tr>
     * <tr><td> decimal </td> <td> BigDecimal</td></tr>
     * <tr><td> integer </td> <td> Integer</td></tr>
     * <tr><td> date </td> <td> java.time.LocalDate</td></tr>
     * <tr><td> timestamp </td> <td> java.time.LocalDateTime</td></tr>
     * <tr><td> time </td> <td> java.time.LocalDateTime</td></tr>
     * </table>
     * 
     * @return next value of the column
     * @throws IOException
     */
    Object next() throws IOException;

}