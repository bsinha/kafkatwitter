package com.ces.kafkatwitter.avro.evolution;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.ces.kafkatwitter.avro.CustomerV1;
import com.ces.kafkatwitter.avro.CustomerV2;

public class EvolutionExample {

	private static void writeUsingVersion1(final CustomerV1 record, final String file) {

		// writing to a file
		final DatumWriter<CustomerV1> writer = new SpecificDatumWriter<CustomerV1>();
		try (DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(writer)) {
			dataFileWriter.create(record.getSchema(), new File(file));
			dataFileWriter.append(record);
			System.out.println("Written  file: " + file);
			dataFileWriter.close();
		} catch (final IOException e) {
			System.out.println("Couldn't write file");
			e.printStackTrace();
		}

	}

	private static void writeUsingVersion2(final CustomerV2 record, final String file) {

		// writing to a file
		final DatumWriter<CustomerV2> writer = new SpecificDatumWriter<CustomerV2>();
		try (DataFileWriter<CustomerV2> dataFileWriter = new DataFileWriter<>(writer)) {
			dataFileWriter.create(record.getSchema(), new File(file));
			dataFileWriter.append(record);
			System.out.println("Written  file: " + file);
			dataFileWriter.close();
		} catch (final IOException e) {
			System.out.println("Couldn't write file");
			e.printStackTrace();
		}

	}

	private static void readUsingVersion1(final String file) {
		// reading from a file
		final DatumReader<CustomerV1> datumReader = new SpecificDatumReader<>(CustomerV1.class);
		CustomerV1 record;
		try (DataFileReader<CustomerV1> dataFileReader = new DataFileReader<>(new File(file), datumReader)) {
			record = dataFileReader.next();
			System.out.println("Successfully read avro file");
			System.out.println(record.toString());

			// get the data from the generic record
			System.out.println("First name: " + record.getFirstName());

		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	private static void readUsingVersion2(final String file) {
		// reading from a file
		final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);
		CustomerV2 record;
		try (DataFileReader<CustomerV2> dataFileReader = new DataFileReader<>(new File(file), datumReader)) {
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
		final CustomerV1 customerV1 = CustomerV1.newBuilder().setAge(34).setAutomatedEmail(false).setFirstName("John")
				.setLastName("Doe").setHeight(178f).setWeight(75f).build();
		System.out.println("Customer V1 = " + customerV1.toString());
		writeUsingVersion1(customerV1, "customer-v1.avro");
		/* Backward Compatible */
		readUsingVersion2("customer-v1.avro");
		System.out.println("Backward Compatibility Successfull!");

		final CustomerV2 customerv2 = CustomerV2.newBuilder().setAge(25).setFirstName("Mark").setLastName("Simpson")
				.setEmail("mark.simpson@gmail.com").setHeight(160f).setWeight(65f).setPhoneNumber("123-456-7890")
				.build();
		System.out.println("Customer V2 = " + customerv2.toString());
		writeUsingVersion2(customerv2, "customer-v2.avro");
		/* Backward Compatible */
		readUsingVersion1("customer-v2.avro");
		System.out.println("Forward Compatibility Successfull!");
	}

}
