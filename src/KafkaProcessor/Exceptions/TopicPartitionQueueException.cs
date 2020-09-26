using Confluent.Kafka;

namespace KafkaProcessor.Exceptions
{
    public class TopicPartitionQueueException : System.Exception
    {
        public TopicPartition TopicPartition { get; set; }

        public TopicPartitionQueueException(
            TopicPartition topicPartition)
        {
            TopicPartition = topicPartition;
        }

        public TopicPartitionQueueException(
            TopicPartition topicPartition,
            string message)
            : base(message)
        {
            TopicPartition = topicPartition;
        }
        public TopicPartitionQueueException(
            TopicPartition topicPartition,
            string message,
            System.Exception inner)
            : base(message, inner)
        {
            TopicPartition = topicPartition;
        }
    }
}