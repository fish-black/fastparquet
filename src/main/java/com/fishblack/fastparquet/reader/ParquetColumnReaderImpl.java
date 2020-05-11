package com.fishblack.fastparquet.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements ParquetColumnReader. It only supports the data types and parquet schema written by fastparquet in parquet
 * conversion.
 */
public class ParquetColumnReaderImpl implements ParquetColumnReader {
    private static final Logger logger = Logger.getLogger(ParquetColumnReaderImpl.class.getName());
    private String parquetPath;
    private String columnName;
    private ParquetFileReader reader;
    private MessageType schema;
    private Type column;
    private ColumnDescriptor columnDescriptor;
    private String columnTypeName;
    private long rowsRead;
    private long totalRows;
    private PageReadStore currentRowGroup;
    private long currentRowGroupPosition;
    private String createdBy;
    private ColumnReader columnReader;

    public ParquetColumnReaderImpl(String parquetPath, String columnName) throws IOException {
        this.parquetPath = parquetPath;
        this.columnName = columnName;
        init();
    }

    private void init() throws IOException {
        logger.log(Level.FINE, "Read column {0} from Parqeut file {1}", new Object[] { columnName, parquetPath });

        reader = ParquetFileReader.open(new Configuration(), new Path(parquetPath));
        schema = reader.getFileMetaData().getSchema();
        
        if(!schema.containsField(columnName)) {
            String message = String.format("Column %s is not found in parquet file %s", columnName, parquetPath);
            throw new IllegalArgumentException(message);
         }
        
        columnDescriptor = schema.getColumnDescription(new String[] { columnName });
        totalRows = reader.getRecordCount();
        createdBy = reader.getFooter().getFileMetaData().getCreatedBy();

        logger.fine("Parquet file schema:" + schema.toString());
        
        column = schema.getType(columnName);
        columnTypeName = "";
        if (column.getOriginalType() != null) {
            columnTypeName = column.getOriginalType().name();
        } else {
            columnTypeName = column.asPrimitiveType().getPrimitiveTypeName().name();
        }

        if(logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Column metadata: columnName={0}, OriginalType={1}, storageType={2}, storageLength={3}",
                    new Object[] { columnName, column.getOriginalType(), columnDescriptor.getType().name(),
                            columnDescriptor.getTypeLength() });
    
            List<BlockMetaData> blockList = reader.getRowGroups();
            for (int i = 0; i < blockList.size(); i++) {
                BlockMetaData block = blockList.get(i);
                logger.log(Level.FINE, "Row group {0} metadata: startPosition={1}, rowCount={2}, size={3}",
                        new Object[] { i, block.getStartingPos(), block.getRowCount(), block.getTotalByteSize() });
            }
        }
    }

    /**
     * @see ParquetColumnReader#hasNext()
     */
    @Override
    public boolean hasNext() {
        return rowsRead < totalRows;
    }

    /**
     * @see ParquetColumnReader#next()
     */
    @Override
    public Object next() throws IOException {
        if (rowsRead >= totalRows) {
            throw new EOFException("End of file reached for parquet file " + parquetPath);
        }

        if (currentRowGroup == null || (currentRowGroupPosition >= currentRowGroup.getRowCount())) {
            // need to switch to next row group
            switchToNextRowGroup();
        }

        Object value;

        int definitionLevel = columnReader.getCurrentDefinitionLevel();

        if (definitionLevel == 0) {
            // it is null
            value = null;
        } else {
            switch (columnTypeName) {
                case "INT32":
                    value = columnReader.getInteger();
                    break;
                case "DOUBLE":
                    value = columnReader.getDouble();
                    break;
                case "DATE":
                    Integer days = columnReader.getInteger();
                    value = LocalDate.ofEpochDay(days);
                    break;
                case "TIMESTAMP_MILLIS":
                    Long timestampVal = columnReader.getLong();
                    value = LocalDateTime.ofEpochSecond(timestampVal / 1000, 0, ZoneOffset.ofHours(0));
                    break;
                case "UTF8":
                    Binary bi = columnReader.getBinary();
                    value = new String(bi.getBytes(), "utf-8");
                    break;
                case "DECIMAL":
                    PrimitiveType ptype = column.asPrimitiveType();
                    DecimalMetadata meta = ptype.getDecimalMetadata();
                    Binary d = columnReader.getBinary();
                    value = new BigDecimal(new BigInteger(d.getBytes()), meta.getScale());
                    break;
                default:
                    // it only supports the data types that OAC uses, throw
                    // exception for other data types
                    String message = String.format("Unsupported data type %s is found in parquet file %s", columnTypeName,
                            parquetPath);
                    throw new IOException(message);
            }
        }
        
        rowsRead++;
        currentRowGroupPosition++;

        // when the column reader initialized, the consume method is already
        // called automatically and points to the 1st record,
        // this is why we call consume method here after the data is read by
        // above code, it moves the pointer to next record
        columnReader.consume();

        return value;
    }

    private void switchToNextRowGroup() throws IOException {
        currentRowGroup = reader.readNextRowGroup();
        ColumnReadStoreImpl columnReaderStore = new ColumnReadStoreImpl(currentRowGroup,
                new DummyGroupConverter(schema), schema, createdBy);
        columnReader = columnReaderStore.getColumnReader(columnDescriptor);
        currentRowGroupPosition = 0;
        long valueCount = columnReader.getTotalValueCount();
        logger.log(Level.FINE, "{0} values in row group for column {1}", new Object[] { valueCount, columnName });
    }

    /**
     * @see ParquetColumnReader#close()
     */
    @Override
    public void close() throws IOException {
        reader.close();
    }

    /**
     * This class is required in order to use the ColumnReadStoreImpl API, but
     * internally we don't reply it for data conversion. The only thing it does
     * is that it returns a DummyPrimitiveConverter for each field.
     */
    private static class DummyGroupConverter extends GroupConverter {
        private final Converter converters[];

        public DummyGroupConverter(GroupType schema) {
            this.converters = new Converter[schema.getFieldCount()];

            int i = 0;
            for (Type field : schema.getFields()) {
                converters[i++] = createConverter(field);
            }
        }

        private Converter createConverter(Type field) {
            return new DummyPrimitiveConverter();
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }

        @Override
        public void start() {

        }

        @Override
        public void end() {

        }
    }

    /**
     * A dummy implementation of PrimitiveConverter used by DummyGroupConverter.
     */
    private static class DummyPrimitiveConverter extends PrimitiveConverter {

    }

}
