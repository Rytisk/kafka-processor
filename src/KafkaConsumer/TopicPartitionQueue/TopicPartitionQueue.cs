using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaConsumer.MessageHandler;

namespace KafkaConsumer.TopicPartitionQueue
{
	public class TopicPartitionQueue<TKey, TValue> : ITopicPartitionQueue<TKey, TValue>
	{
		private readonly IMessageHandler<TKey, TValue> _messageHandler;

		public TopicPartitionQueue(IMessageHandler<TKey, TValue> messageHandler)
		{
			_messageHandler = messageHandler;
		}

		public Task EnqueueAsync(ConsumeResult<TKey, TValue> consumeResult)
		{
			//TODO: use Dataflow blocks

			throw new NotImplementedException();
		}
	}
}