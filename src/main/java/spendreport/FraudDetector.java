/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final Logger LOG = LoggerFactory.getLogger(FraudDetector.class);


	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	private transient ValueState<Boolean> flagState;

	private transient ValueState<Long> timerState;

	@Override
	public void open(OpenContext openContext) {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// Get the current state for the current key
		Boolean lastTransactionWasSmall = flagState.value();

		// Check if the flag is set
		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				LOG.info("FRAUD pattern detected Key {}, this amount {}} @ {} / {}", context.getCurrentKey(), transaction.getAmount(), new Date(transaction.getTimestamp()), new Date(context.timestamp()) );
				//Output an alert downstream
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());
				alert.setTimestamp(context.timestamp());

				collector.collect(alert);
			}
			// Clean up our state
			cleanUp(context);
		}

		if (transaction.getAmount() < SMALL_AMOUNT) {
			// set the flag to true
			LOG.info("KWDEBUG Key {}, Amount is small {}} @ {}", context.getCurrentKey(), transaction.getAmount(), new Date(transaction.getTimestamp()) );

			flagState.update(true);

			long l = context.timerService().currentProcessingTime();
			LOG.debug("KWDEBUG current processing time {}", new Date(l));
			long timer = l + ONE_MINUTE;

			context.timerService().registerProcessingTimeTimer(timer);

			timerState.update(timer);
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
		// remove flag after 1 minute
		LOG.info("KWDEBUG timeout for key {}", ctx.getCurrentKey());
		timerState.clear();
		flagState.clear();
	}

	private void cleanUp(Context ctx) throws Exception {
		// delete timer
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		// clean up all state
		timerState.clear();
		flagState.clear();
	}}
