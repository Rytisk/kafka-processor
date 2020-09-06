using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaConsumer.TopicPartitionQueue;

namespace KafkaConsumer.Processor
{
	public class KafkaProcessor<TKey, TValue> : IKafkaProcessor<TKey, TValue>
	{
		private readonly IConsumer<TKey, TValue> _consumer;
		private readonly ITopicPartitionQueueSelector<TKey, TValue> _topicPartitionQueueSelector;

		public KafkaProcessor(
			IConsumer<TKey, TValue> consumer,
			ITopicPartitionQueueSelector<TKey, TValue> topicPartitionQueueSelector)
		{
			_consumer = consumer;
			_topicPartitionQueueSelector = topicPartitionQueueSelector;
		}

		public async Task ProcessMessagesAsync(CancellationToken ct)
		{
			try
			{
				while (!ct.IsCancellationRequested)
				{
					var consumeResult = _consumer.Consume(ct);

					var queue = _topicPartitionQueueSelector.Select(consumeResult.TopicPartition);

					await queue.EnqueueAsync(consumeResult);
				}
			}
			catch (OperationCanceledException) { }
		}
	}
}