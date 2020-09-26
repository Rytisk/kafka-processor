using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KafkaProcessor.TopicPartitionQueue;

namespace KafkaProcessor.Processor
{
    public class KafkaProcessor<TKey, TValue> : IKafkaProcessor<TKey, TValue>
    {
        private readonly IConsumer<TKey, TValue> _consumer;
        private readonly ITopicPartitionQueueSelector<TKey, TValue> _topicPartitionQueueSelector;
        private readonly string _topic;

        public KafkaProcessor(
            IConsumer<TKey, TValue> consumer,
            ITopicPartitionQueueSelector<TKey, TValue> topicPartitionQueueSelector,
            string topic)
        {
            _consumer = consumer;
            _topicPartitionQueueSelector = topicPartitionQueueSelector;
            _topic = topic;
        }

        public async Task ProcessMessagesAsync(CancellationToken ct)
        {
            _consumer.Subscribe(_topic);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var consumeResult = Consume(ct);

                    if (consumeResult != null)
                    {
                        var queue = _topicPartitionQueueSelector.Select(consumeResult.TopicPartition);

                        var message = new MessageHandler.Message<TKey, TValue>(_consumer, consumeResult);

                        await queue.EnqueueAsync(message);
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                //TODO: Close() blocks indefinitely if an exception is thrown in PartitionsRevoked/Assigned handlers.
                // https://github.com/confluentinc/confluent-kafka-dotnet/issues/1280

                _consumer.Close();
            }
        }

        private ConsumeResult<TKey, TValue> Consume(CancellationToken ct)
        {
            ConsumeResult<TKey, TValue> cr = null;

            try
            {
                cr = _consumer.Consume(ct);
            }
            catch (ConsumeException)
            {
                //TODO: log and continue - ConsumeExceptions are not fatal
            }

            return cr;
        }

        public void Dispose()
        {
            _consumer.Dispose();
        }
    }
}