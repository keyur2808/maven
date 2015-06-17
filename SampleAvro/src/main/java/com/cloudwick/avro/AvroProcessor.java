/**
 * Resource: https://avro.apache.org/docs/current/gettingstartedjava.html
 */

package com.cloudwick.avro;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroProcessor {
	private static final String serializedFilePath;
	private static final String deserializedFilePath;

	static {
		serializedFilePath = "outputs/serialized.avro";
		deserializedFilePath = "outputs/deserialized.txt";
	}

	public static void main(String... args) throws IOException {
		/**
		 * Create 3 sample Employee objects to be serialized
		 */

		Employee emp1 = new Employee();
		emp1.setId(101);
		emp1.setName("Rhonda");
		emp1.setDesignation("Hadoop Engineer");
		emp1.setMgrid(25);
		emp1.setHiredate("05-18-2015");
		emp1.setSalary(100000d);
		emp1.setDeptid(21);

		Employee emp2 = Employee.newBuilder().setId(201).setName("Salena")
				.setDesignation("Java Programmer").setMgrid(73)
				.setHiredate("03-19-2016").setSalary(102000d).setDeptid(73)
				.setCommission(2242d).build();

		Employee emp3 = new Employee(2914, "Melissa", "Marketing Manager",
				8910, "12-12-2014", 434351d, null, 343);

		/**
		 * DatumWriter converts Java objects into an in-memory serialized form
		 * The SpecificDatumWriter class is used with generated classes and
		 * extracts the schema from the specified generated type
		 * 
		 * DataFileWriter is created, which writes the serialized records, as
		 * well as schema, to the file specified in dataFileWriter.create call.
		 * Write the users to the file via calls to the dataFileWriter.append
		 * method. When done writing, close the file.
		 */

		DatumWriter<Employee> userDatumWriter = new SpecificDatumWriter<Employee>(
				Employee.class);
		DataFileWriter<Employee> dataFileWriter = new DataFileWriter<Employee>(
				userDatumWriter);
		dataFileWriter.create(emp1.getSchema(), new File(serializedFilePath));
		dataFileWriter.append(emp1);
		dataFileWriter.append(emp2);
		dataFileWriter.append(emp3);
		dataFileWriter.close();

		/**
		 * Read the .avro file (serialized) created in the above part of the
		 * program, and create the de-serialized file (.json format) and store
		 * on the disk
		 */

		DatumReader<Employee> userDatumReader = new SpecificDatumReader<Employee>(
				Employee.class);
		DataFileReader<Employee> dataFileReader = new DataFileReader<Employee>(
				new File(serializedFilePath), userDatumReader);

		BufferedWriter writer = new BufferedWriter(new FileWriter(
				deserializedFilePath));
		Employee emp = null;
		while (dataFileReader.hasNext()) {
			emp = dataFileReader.next(emp);
			writer.write(emp.toString() + "\n");
		}

		writer.close();
		dataFileReader.close();

		System.out.println("Process Complete!!");
		System.out
				.println("Check output/serialized.avro and output/deserialized.txt");
	}
}