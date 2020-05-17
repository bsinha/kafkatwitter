package com.ces.kafkatwitter.avro.reflect;

import org.apache.avro.reflect.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
	private String firstName;
	private String lastName;
	private int age;

	@Nullable
	private String nickName;

}
