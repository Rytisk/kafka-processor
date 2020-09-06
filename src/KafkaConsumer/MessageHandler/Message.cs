using Confluent.Kafka;

namespace KafkaConsumer.MessageHandler
{
	public class Message<TKey, TValue>
	{
		public ConsumeResult<TKey, TValue> ConsumeResult { get; set; }

		public void StoreOffset()
		{
			//TODO: call on IConsumer
		}

		public void Commit()
		{
			//TODO: call on IConsumer
		}
	}
}