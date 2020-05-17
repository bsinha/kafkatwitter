package com.ces.kafkatwitter.avro.specific;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.ces.kafkatwitter.avro.Customer;

public class SpecificRecordExample {

	private static void writeToFile(final Customer record, final String file) {

		// writing to a file
		final DatumWriter<Customer> writer = new SpecificDatumWriter<Customer>();
		try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(writer)) {
			dataFileWriter.create(record.getSchema(), new File(file));
			dataFileWriter.append(record);
			System.out.println("Written  file: " + file);
			dataFileWriter.close();
		} catch (final IOException e) {
			System.out.println("Couldn't write file");
			e.printStackTrace();
		}

	}

	private static void readFromFile(final String file) {
		// reading from a file
		final DatumReader<Customer> datumReader = new SpecificDatumReader<>();
		Customer record;
		try (DataFileReader<Customer> dataFileReader = new DataFileReader<>(new File(file), datumReader)) {
			record = dataFileReader.next();
			System.out.println("Successfully read avro file");
			System.out.println(record.toString());

			// get the data from the generic record
			System.out.println("First name: " + record.getFirstName());

		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(final String... strings) {

		final Customer customer = Customer.newBuilder().setFirstName("John").setLastName("Doe").setAge(32)
				.setHeight(180f).setWeight(75f).setAutomatedEmail(false).build();

		writeToFile(customer, "customer-specific.avro");
		readFromFile("customer-specific.avro");

		final Customer customerWithDefault = Customer.newBuilder().setFirstName("John").setLastName("Doe").setAge(32)
				.setHeight(180f).setWeight(75f).build();
		writeToFile(customerWithDefault, "customer-default-specific.avro");
		readFromFile("customer-default-specific.avro");
	}

}
