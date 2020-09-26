using KafkaProcessor.MessageHandler;
using KafkaProcessor.Extensions.Sample.MessageHandlers.Extensions;
using System;
using System.Threading.Tasks;

namespace KafkaProcessor.Extensions.Sample.MessageHandlers
{
    public class SimpleMessageHandler : IMessageHandler<string, string>
    {
        public SimpleMessageHandler()
        {
            Console.WriteLine($"SimpleHandler created");
        }

        public async Task HandleAsync(Message<string, string> message)
        {
            await ProcessMessageAsync(message);

            message.StoreOffset();
        }

        private async Task ProcessMessageAsync(Message<string, string> message)
        {
            (var key, var value) = message.ConsumeResult.Message;

            Console.WriteLine("SIMPLE: " +
                $"Processing a message - {message.ConsumeResult.TopicPartitionOffset} - " +
                $"Key='{key}', " +
                $"Value='{value}'");

            await Task.Delay(10);
        }
    }
}
