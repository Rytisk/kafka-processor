using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using KafkaConsumer.MessageHandler;
using KafkaConsumer.Processor.Config;
using KafkaConsumer.TopicPartitionQueue;
using Microsoft.Extensions.Options;

namespace KafkaConsumer.Processor
{
	public class KafkaProcessorBuilder<TKey, TValue>
	{
		private ITopicPartitionQueueSelector<TKey, TValue> _topicPartitionQueueSelector;
		private Func<TopicPartition, IMessageHandler<TKey, TValue>> _handlerFactory;
		private ConsumerConfig _config;
		private string _topic;

		public KafkaProcessorBuilder()
		{
		}

		public KafkaProcessorBuilder<TKey, TValue> WithConfig(ConsumerConfig config)
		{
			_config = config;

			return this;
		}

		public KafkaProcessorBuilder<TKey, TValue> FromTopic(string topic)
		{
			_topic = topic;

			return this;
		}

		public KafkaProcessorBuilder<TKey, TValue> WithHandlerFactory(Func<TopicPartition, IMessageHandler<TKey, TValue>> handlerFactory)
		{
			_handlerFactory = handlerFactory;

			return this;
		}

		public IKafkaProcessor<TKey, TValue> Build()
		{
			var queueFactory = new TopicPartitionQueueFactory<TKey, TValue>();
			
			_topicPartitionQueueSelector = new TopicPartitionQueueSelector<TKey, TValue>(queueFactory);

			var consumer = new ConsumerBuilder<TKey, TValue>(_config)
				.SetPartitionsAssignedHandler(OnPartitionsAssigned)
				.SetPartitionsRevokedHandler(OnPartitionsRevoked)
				.Build();

			//TODO: Value/Key deserializers for consumer

			var config = Options.Create(new ProcessorConfig
			{
				Topic = _topic
			});

			return new KafkaProcessor<TKey, TValue>(
				consumer,
				_topicPartitionQueueSelector,
				config);
		}

		private IEnumerable<TopicPartitionOffset> OnPartitionsRevoked(
			IConsumer<TKey, TValue> consumer,
			List<TopicPartitionOffset> partitions)
		{
			_topicPartitionQueueSelector.Remove(partitions.Select(p => p.TopicPartition));

			try
			{
				consumer.Commit();
			}
			catch (KafkaException ex)
				when (ex.Error.Code == ErrorCode.Local_NoOffset)
			{
				// ignore 
			}

			return partitions;
		}

		private IEnumerable<TopicPartitionOffset> OnPartitionsAssigned(
			IConsumer<TKey, TValue> consumer,
			List<TopicPartition> partitions)
		{
			foreach (var partition in partitions)
			{
				var messageHandler = _handlerFactory.Invoke(partition);

				_topicPartitionQueueSelector.AddQueue(partition, messageHandler);
			}

			return partitions.Select(p => new TopicPartitionOffset(p, Offset.Stored));
		}
	}
}