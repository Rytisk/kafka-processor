using KafkaConsumer.MessageHandler;

namespace KafkaConsumer.TopicPartitionQueue
{
	public class TopicPartitionQueueFactory<TKey, TValue> : ITopicPartitionQueueFactory<TKey, TValue>
	{
		public TopicPartitionQueueFactory()
		{
		}

		public ITopicPartitionQueue<TKey, TValue> Create(IMessageHandler<TKey, TValue> messageHandler)
		{
			return new TopicPartitionQueue<TKey, TValue>(messageHandler);
		}
	}
}