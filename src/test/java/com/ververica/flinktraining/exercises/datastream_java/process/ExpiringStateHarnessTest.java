/*
 * Copyright 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.process;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.solutions.datastream_java.process.ExpiringStateSolution;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

public class ExpiringStateHarnessTest {

	@Test
	public void testRideThenFare() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> harness = setupHarness();

		final TaxiRide ride1 = testRide(1);
		final TaxiFare fare1 = testFare(1);

		final long timeForFare = 1L;

		harness.processElement1(new StreamRecord<>(ride1, 0));
		harness.processElement2(new StreamRecord<>(fare1, timeForFare));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new StreamRecord<>(new Tuple2<>(ride1, fare1), timeForFare));

		// Check that the result is correct
		ConcurrentLinkedQueue<Object> actualOutput = harness.getOutput();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

		// Check that no state or timers are left behind
		assert(harness.numKeyedStateEntries() == 0);
		assert(harness.numEventTimeTimers() == 0);
	}

	@Test
	public void testLateEvent() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> harness = setupHarness();

		final TaxiRide ride1 = testRide(1);
		final TaxiFare fare1 = testFare(1);

		harness.processElement1(new StreamRecord<>(ride1, 0));

		// Check that 1 state and 1 timers are created for the ride event
		assert(harness.numKeyedStateEntries() == 1);
		assert(harness.numEventTimeTimers() == 1);

		// advance the ride & fare watermark
		harness.processBothWatermarks(new Watermark(1));

		// Check that no state or timers are left behind
		assert(harness.numKeyedStateEntries() == 0);
		assert(harness.numEventTimeTimers() == 0);

		// push a late fare event
		harness.processElement2(new StreamRecord<>(fare1, 2));

		// Check that 1 state and 1 timers are created for the late fare event
		assert(harness.numKeyedStateEntries() == 1);
		assert(harness.numEventTimeTimers() == 1);

		// advance the side & fare watermark
		harness.processBothWatermarks(new Watermark(2));

		// Check that no state or timers are left behind
		assert(harness.numKeyedStateEntries() == 0);
		assert(harness.numEventTimeTimers() == 0);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new Watermark(1));
		expectedOutput.add(new Watermark(2));

		// Check that the result contains only two watermarks as the unmatched (due to lateness) events are sent to side output
		ConcurrentLinkedQueue<Object> actualOutput = harness.getOutput();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

	}


	private TaxiRide testRide(long rideId) {
		return new TaxiRide(rideId, true, new DateTime(0), new DateTime(0),
				0F, 0F, 0F, 0F, (short)1, 0, rideId);
	}

	private TaxiFare testFare(long rideId) {
		return new TaxiFare(rideId, 0, rideId, new DateTime(0), "", 0F, 0F, 0F);
	}

	private KeyedTwoInputStreamOperatorTestHarness<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> setupHarness() throws Exception {
		// instantiate operator
		KeyedCoProcessOperator<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> operator =
				new KeyedCoProcessOperator<>(new ExpiringStateSolution.EnrichmentFunction());

		// setup test harness
		KeyedTwoInputStreamOperatorTestHarness<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> testHarness =
				new KeyedTwoInputStreamOperatorTestHarness<>(operator,
						(TaxiRide r) -> r.rideId,
						(TaxiFare f) -> f.rideId,
						BasicTypeInfo.LONG_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		return testHarness;
	}
}
