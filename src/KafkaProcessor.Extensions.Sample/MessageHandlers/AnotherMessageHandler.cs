using System;
using System.Threading.Tasks;
using KafkaProcessor.MessageHandler;
using KafkaProcessor.Extensions.Sample.MessageHandlers.Extensions;

namespace KafkaProcessor.Extensions.Sample.MessageHandlers
{
    public class AnotherMessageHandler: IMessageHandler<string, string>
    {
        public AnotherMessageHandler()
        {
            Console.WriteLine($"AnotherHandler created");
        }

        public async Task HandleAsync(Message<string, string> message)
        {
            await ProcessMessageAsync(message);

            message.StoreOffset();
        }

        private async Task ProcessMessageAsync(Message<string, string> message)
        {
            (var key, var value) = message.ConsumeResult.Message;

            Console.WriteLine("ANOTHER: " +
                $"Processing a message - {message.ConsumeResult.TopicPartitionOffset} - " +
                $"Key='{key}', " +
                $"Value='{value}'");

            await Task.Delay(10);
        }
    }
}
