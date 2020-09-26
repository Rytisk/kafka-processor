using Confluent.Kafka;

namespace KafkaProcessor.Processor.Config
{
    public class ProcessorConfig
    {
        public string Topic { get; set; }
        public ConsumerConfig ConsumerConfig { get; set; }
    }
}