using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace KafkaConsumer.Tests
{
	public static class DataGenerator
	{

		public static IEnumerable<TopicPartition> GenerateTopicPartitions(int count) =>
			Enumerable
				.Range(0, count)
				.Select(i => new TopicPartition("test-topic", i));

		public static TopicPartition TopicPartition =>
					new TopicPartition("test-topic", 1);

		public static TopicPartitionOffset TopicPartitionOffset =>
			new TopicPartitionOffset(TopicPartition, 1);

		public static MessageHandler.Message<string, string> GenerateMessage(IConsumer<string, string> consumer) =>
			new MessageHandler.Message<string, string>(consumer, ConsumeResult);

		public static ConsumeResult<string, string> ConsumeResult =>
			new ConsumeResult<string, string>
			{
				Message = new Message<string, string>
				{
					Key = "key",
					Value = "value"
				},
				TopicPartitionOffset = TopicPartitionOffset
			};
	}
}