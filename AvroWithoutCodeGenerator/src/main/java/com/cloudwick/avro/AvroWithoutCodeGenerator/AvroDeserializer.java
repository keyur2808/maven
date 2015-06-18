package com.cloudwick.avro.AvroWithoutCodeGenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroDeserializer {
	public static void main(String... args) throws IOException {
		Schema schema = new Schema.Parser().parse(new File(
				"src/main/resources/schema/schema.avsc"));
		GenericRecord record = new GenericData.Record(schema);

		File file = new File("output/serialized.avro");

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				file, datumReader);

		File file2 = new File("output/deserialized.txt");

		if (!file2.exists()) {
			file2.createNewFile();
		}

		FileWriter fileWriter = new FileWriter(file2.getAbsoluteFile());
		BufferedWriter writer = new BufferedWriter(fileWriter);

		while (dataFileReader.hasNext()) {
			record = dataFileReader.next();
			writer.write(record.toString() + "\n");
		}

		writer.close();
		dataFileReader.close();
	}
}