using Confluent.Kafka;

namespace KafkaProcessor.MessageHandler
{
    public class Message<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> _consumer;

        public ConsumeResult<TKey, TValue> ConsumeResult { get; set; }

        public Message(IConsumer<TKey, TValue> consumer, ConsumeResult<TKey, TValue> consumeResult)
        {
            _consumer = consumer;

            ConsumeResult = consumeResult;
        }

        public void StoreOffset()
        {
            _consumer.StoreOffset(ConsumeResult);
        }

        public void Commit()
        {
            _consumer.Commit(ConsumeResult);
        }
    }
}