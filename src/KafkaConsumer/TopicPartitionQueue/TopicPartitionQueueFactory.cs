using KafkaConsumer.MessageHandler;

namespace KafkaConsumer.TopicPartitionQueue
{
	public class TopicPartitionQueueFactory<TKey, TValue> : ITopicPartitionQueueFactory<TKey, TValue>
	{
		private readonly IMessageHandler<TKey, TValue> _messageHandler;

		public TopicPartitionQueueFactory(IMessageHandler<TKey, TValue> messageHandler)
		{
			_messageHandler = messageHandler;
		}

		public ITopicPartitionQueue<TKey, TValue> Create()
		{
			return new TopicPartitionQueue<TKey, TValue>(_messageHandler);
		}
	}
}