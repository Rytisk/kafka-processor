using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace KafkaProcessor.Tests
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

		public static MessageHandler.Message<string, string> GenerateMessage(
			IConsumer<string, string> consumer) =>
			new MessageHandler.Message<string, string>(consumer, ConsumeResult);

		public static IEnumerable<MessageHandler.Message<string, string>> GenerateMessages(
			IConsumer<string, string> consumer, 
			int count) => 
			GenerateConsumeResults(count)
				.Select(cr => new MessageHandler.Message<string, string>(consumer, cr));

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
		
		public static IEnumerable<ConsumeResult<string, string>> GenerateConsumeResults(int count) =>
			Enumerable.Range(0, count).Select(i => new ConsumeResult<string, string>
			{
				Message = new Message<string, string>
				{
					Key = $"key_{i}",
					Value = $"value_{i}"
				},
				TopicPartitionOffset = new TopicPartitionOffset("test-topic", 0, i)
			});
	}
}