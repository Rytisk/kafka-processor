using Confluent.Kafka;

namespace KafkaConsumer.TopicPartitionQueue
{
	public class TopicPartitionQueueSelector<TKey, TValue> : ITopicPartitionQueueSelector<TKey, TValue>
	{
		public ITopicPartitionQueue<TKey, TValue> Select(TopicPartition topicPartition)
		{
			throw new System.NotImplementedException();
		}
	}
}