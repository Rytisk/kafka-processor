using Confluent.Kafka;

namespace KafkaConsumer.TopicPartitionQueue
{
	public interface ITopicPartitionQueueSelector<TKey, TValue>
	{
		ITopicPartitionQueue<TKey, TValue> Select(TopicPartition topicPartition);
	}
}