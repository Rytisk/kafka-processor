using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaConsumer.Processor.Config;
using KafkaConsumer.TopicPartitionQueue;
using Microsoft.Extensions.Options;

namespace KafkaConsumer.Processor
{
	public class KafkaProcessor<TKey, TValue> : IKafkaProcessor<TKey, TValue>
	{
		private readonly IConsumer<TKey, TValue> _consumer;
		private readonly ITopicPartitionQueueSelector<TKey, TValue> _topicPartitionQueueSelector;
		private readonly ProcessorConfig _config;

		public KafkaProcessor(
			IConsumer<TKey, TValue> consumer,
			ITopicPartitionQueueSelector<TKey, TValue> topicPartitionQueueSelector,
			IOptions<ProcessorConfig> config)
		{
			_consumer = consumer;
			_topicPartitionQueueSelector = topicPartitionQueueSelector;
			_config = config.Value;
		}

		public async Task ProcessMessagesAsync(CancellationToken ct)
		{
			_consumer.Subscribe(_config.Topic);

			try
			{
				while (!ct.IsCancellationRequested)
				{
					var consumeResult = Consume(ct);

					if (consumeResult != null)
					{
						var queue = _topicPartitionQueueSelector.Select(consumeResult.TopicPartition);

						await queue.EnqueueAsync(consumeResult);
					}
				}
			}
			catch (OperationCanceledException) { }
			finally
			{
				_consumer.Close();
			}
		}

		private ConsumeResult<TKey, TValue> Consume(CancellationToken ct)
		{
			ConsumeResult<TKey, TValue> cr = null;

			try
			{
				cr = _consumer.Consume(ct);
			}
			catch (ConsumeException)
			{
				//TODO: log and continue - ConsumeExceptions are not fatal
			}

			return cr;
		}

		public void Dispose()
		{
			_consumer.Dispose();
		}
	}
}