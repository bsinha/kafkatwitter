package com.ces.kafkatwitter.avro.reflect;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class ReflectionExample {

	private static void writeToFile(final Person person, final String file) {
		// here we use reflection to determine the schema
		final Schema schema = ReflectData.get().getSchema(Person.class);
		System.out.println("schema = " + schema.toString(true));
		// writing to a file
		final DatumWriter<Person> writer = new ReflectDatumWriter<Person>();
		try (DataFileWriter<Person> dataFileWriter = new DataFileWriter<>(writer)) {
			dataFileWriter.setCodec(CodecFactory.deflateCodec(9)).create(schema, new File(file));

			dataFileWriter.append(person);
			System.out.println("Written  file: " + file);
			dataFileWriter.close();
		} catch (final IOException e) {
			System.out.println("Couldn't write file");
			e.printStackTrace();
		}
	}

	private static void readFromFile(final String file) {
		// reading from a file
		final DatumReader<Person> datumReader = new ReflectDatumReader<>(Person.class);
		Person record;
		try (DataFileReader<Person> dataFileReader = new DataFileReader<>(new File(file), datumReader)) {
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
		final Person person = Person.builder().firstName("John").lastName("Doe").age(21).nickName("Jack").build();
		writeToFile(person, "person-reflected.avro");
		readFromFile("person-reflected.avro");
	}

}
