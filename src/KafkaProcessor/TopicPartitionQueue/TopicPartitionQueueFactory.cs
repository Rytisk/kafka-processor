using KafkaProcessor.MessageHandler;

namespace KafkaProcessor.TopicPartitionQueue
{
	public class TopicPartitionQueueFactory<TKey, TValue> : ITopicPartitionQueueFactory<TKey, TValue>
	{
		public ITopicPartitionQueue<TKey, TValue> Create(
			IMessageHandler<TKey, TValue> messageHandler,
			int queueCapacity)
		{
			return new TopicPartitionQueue<TKey, TValue>(messageHandler, queueCapacity);
		}
	}
}