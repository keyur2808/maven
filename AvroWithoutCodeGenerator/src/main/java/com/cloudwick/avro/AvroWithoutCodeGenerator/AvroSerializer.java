/**
 * https://hadooptutorial.info/avro-serializing-and-deserializing-example-java-api/
 * Take user defined input to create a .avro file
 */

package com.cloudwick.avro.AvroWithoutCodeGenerator;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroSerializer {
	public static void main(String... args) throws IOException {
		// Read the .avsc file and serialize it
		Schema schema = new Schema.Parser().parse(new File(
				"src/main/resources/schema/schema.avsc"));

		// Create a GenericRecord object to create new data
		GenericRecord record1 = new GenericData.Record(schema);
		record1.put("name", "Sydney");
		record1.put("salary", 140000d);
		record1.put("id", 3810);
		record1.put("hiredate", "02-06-1990");
		record1.put("deptid", 8302);
		record1.put("designation", "Spark Developer");
		record1.put("mgrid", 83291);
		record1.put("commission", 882d);

		GenericRecord record2 = new GenericData.Record(schema);
		record2.put("name", "Herold");
		record2.put("salary", 73900d);
		record2.put("id", 1739);
		record2.put("hiredate", "12-16-1972");
		record2.put("deptid", 7719);
		record2.put("designation", "Cassandra Admin");
		record2.put("mgrid", 77291);
		record2.put("commission", 0d);

		File file = new File("output/serialized.avro");
		DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<GenericRecord>(
				schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(record1);
		dataFileWriter.append(record2);
		dataFileWriter.close();
	}
}