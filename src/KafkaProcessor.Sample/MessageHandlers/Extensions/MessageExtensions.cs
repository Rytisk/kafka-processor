using Confluent.Kafka;

namespace KafkaProcessor.Sample.MessageHandlers.Extensions
{
	public static class MessageExtensions
	{
		public static void Deconstruct<TKey, TValue>(
			this Message<TKey, TValue> message,
			out TKey Key,
			out TValue Value)
		{
			Key = message.Key;
			Value = message.Value;
		}
	}
}
