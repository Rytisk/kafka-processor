using KafkaConsumer.MessageHandler;
using KafkaConsumer.Sample.Extensions;
using System;
using System.Threading.Tasks;

namespace KafkaConsumer.Sample.MessageHandlers
{
	public class SimpleMessageHandler : IMessageHandler<string, string>
	{
		private readonly string _topic;
		private readonly int _partition;

		public SimpleMessageHandler(string topic, int partition)
		{
			_topic = topic;
			_partition = partition;

			Console.WriteLine($"Created a message handler for: {topic}<{partition}>");
		}

		public async Task HandleAsync(Message<string, string> message)
		{
			await ProcessMessageAsync(message);

			message.StoreOffset();
		}

		private async Task ProcessMessageAsync(Message<string, string> message)
		{
			(var key, var value) = message.ConsumeResult.Message;

			Console.WriteLine($"[Handler:{_topic}<{_partition}>] " +
				$"Processing a message - {message.ConsumeResult.TopicPartitionOffset} - " +
				$"Key='{key}', " +
				$"Value='{value}'");

			await Task.Delay(10);
		}
	}
}
