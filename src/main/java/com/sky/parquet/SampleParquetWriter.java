package com.sky.parquet;


import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleParquetWriter extends ParquetWriter<List<String>> {
    public SampleParquetWriter(Path file, MessageType schema) throws IOException {
        this(file, schema, false);
    }

    public SampleParquetWriter(Path file, MessageType schema, boolean enableDictionary) throws IOException {
        this(file, schema, CompressionCodecName.UNCOMPRESSED, enableDictionary);
    }

    public SampleParquetWriter(Path file, MessageType schema, CompressionCodecName codecName, boolean enableDictionary) throws IOException {
        super(file, (WriteSupport<List<String>>) new SampleParquetWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
    }

    public static MessageType getSchema(List<String[]> columns){

        List<Type> types=new ArrayList<Type>();
        Type type=null;

        for (int i = 0; i < columns.size() ; i++) {
            String[] cols=columns.get(i);
            type=new PrimitiveType(Type.Repetition.OPTIONAL,PrimitiveType.PrimitiveTypeName.BINARY,cols[0], OriginalType.UTF8);
            types.add(type);
        }
        return new MessageType("Ec-schema",types);
    }


    public static void main(String[] args) throws IOException{

        File outputParquetFile = new File("/home/syadav/junk/abc.parquet");
        Path path = new Path(outputParquetFile.toURI());
        List cols= new ArrayList<String[]>();
        cols.add(new String[]{"id"});
        cols.add(new String[]{"name"});
        MessageType schema = getSchema(cols);
        SampleParquetWriter writer = new SampleParquetWriter(path,schema);
        writer.write(Arrays.asList(new String[]{"1", "Sunil"}));
        writer.write(Arrays.asList(new String[]{"2", "Yadav"}));
        writer.close();

    }
}
