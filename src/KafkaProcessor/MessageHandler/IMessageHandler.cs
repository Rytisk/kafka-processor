using System.Threading.Tasks;

namespace KafkaProcessor.MessageHandler
{
    public interface IMessageHandler<TKey, TValue>
    {
        Task HandleAsync(Message<TKey, TValue> message);
    }
}