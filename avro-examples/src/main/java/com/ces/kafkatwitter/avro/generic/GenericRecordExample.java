package com.ces.kafkatwitter.avro.generic;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericRecordExample {
	private static final Schema.Parser parser = new Schema.Parser();

	public static void writeToFile(final GenericData.Record record, final String file) {

		// writing to a file
		final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
		try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			dataFileWriter.create(record.getSchema(), new File(file));
			dataFileWriter.append(record);
			System.out.println("Written  file: " + file);
			dataFileWriter.close();
		} catch (final IOException e) {
			System.out.println("Couldn't write file");
			e.printStackTrace();
		}

	}

	public static void readFromFile(final String file) {
		// reading from a file
		final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
		GenericRecord record;
		try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(file), datumReader)) {
			record = dataFileReader.next();
			System.out.println("Successfully read avro file");
			System.out.println(record.toString());

			// get the data from the generic record
			System.out.println("First name: " + record.get("first_name"));

			// read a non existent field
			System.out.println("Non existent field: " + record.get("not_here"));
		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(final String... strings) {
		final Schema schema = parser.parse("{\r\n" + "     \"type\": \"record\",\r\n"
				+ "     \"namespace\": \"com.ces.kafkatwitter.avro\",\r\n" + "     \"name\": \"Customer\",\r\n"
				+ "     \"fields\": [\r\n"
				+ "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\r\n"
				+ "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\r\n"
				+ "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\r\n"
				+ "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\r\n"
				+ "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\r\n"
				+ "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\r\n"
				+ "     ]\r\n" + "}");
		final GenericData.Record customer = new GenericRecordBuilder(schema).set("first_name", "John")
				.set("last_name", "Doe").set("age", 23).set("height", 175f).set("weight", 60f)
				.set("automated_email", false).build();
		writeToFile(customer, "customer-generic.avro");
		readFromFile("customer-generic.avro");

		final GenericData.Record customerWithDefault = new GenericRecordBuilder(schema).set("first_name", "Jane")
				.set("last_name", "Doe").set("age", 1).set("height", 165f).set("weight", 50f).build();
		writeToFile(customerWithDefault, "customer-default-generic.avro");
		readFromFile("customer-default-generic.avro");

	}
}
