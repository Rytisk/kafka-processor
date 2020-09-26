using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using KafkaProcessor.MessageHandler;
using KafkaProcessor.Processor.Config;
using KafkaProcessor.TopicPartitionQueue;
using Microsoft.Extensions.Options;

namespace KafkaProcessor.Processor
{
    public class KafkaProcessorBuilder<TKey, TValue>
    {
        private readonly ITopicPartitionQueueSelector<TKey, TValue> _topicPartitionQueueSelector;

        private Func<TopicPartition, IMessageHandler<TKey, TValue>> _handlerFactory;
        private ConsumerConfig _consumerConfig;
        private string _topic;
        private IDeserializer<TKey> _keyDeserializer;
        private IDeserializer<TValue> _valueDeserializer;

        public KafkaProcessorBuilder(ITopicPartitionQueueSelector<TKey, TValue> topicPartitionQueueSelector)
        {
            _topicPartitionQueueSelector = topicPartitionQueueSelector;
        }

        public static KafkaProcessorBuilder<TKey, TValue> CreateDefault()
        {
            var queueFactory = new TopicPartitionQueueFactory<TKey, TValue>();

            var topicPartitionQueueSelector = new TopicPartitionQueueSelector<TKey, TValue>(
                queueFactory,
                1000);

            return new KafkaProcessorBuilder<TKey, TValue>(topicPartitionQueueSelector);
        }

        public KafkaProcessorBuilder<TKey, TValue> WithConfig(ConsumerConfig consumerConfig)
        {
            if (_consumerConfig != null) 
                throw new InvalidOperationException("'consumerConfig' was already set!");

            _consumerConfig = consumerConfig;

            return this;
        }

        public KafkaProcessorBuilder<TKey, TValue> FromTopic(string topic)
        {
            if (_topic != null) 
                throw new InvalidOperationException("'topic' was already set!");

            _topic = topic;

            return this;
        }

        public KafkaProcessorBuilder<TKey, TValue> WithHandlerFactory(
            Func<TopicPartition, IMessageHandler<TKey, TValue>> handlerFactory)
        {
            if (_handlerFactory != null) 
                throw new InvalidOperationException("'handlerFactory' was already set!");
            
            _handlerFactory = handlerFactory;

            return this;
        }

        public KafkaProcessorBuilder<TKey, TValue> WithKeyDeserializer(IDeserializer<TKey> keyDeserializer)
        {
            if (_keyDeserializer != null)
                throw new InvalidOperationException("'keyDeserializer' was already set!");

            _keyDeserializer = keyDeserializer;
            
            return this;
        }

        public KafkaProcessorBuilder<TKey, TValue> WithValueDeserializer(IDeserializer<TValue> valueDeserializer)
        {
            if (_valueDeserializer != null)
                throw new InvalidOperationException("'valueDeserializer' was already set!");

            _valueDeserializer = valueDeserializer;

            return this;
        }

        public IKafkaProcessor<TKey, TValue> Build()
        {
            CheckIfConfigured();

            var consumer = BuildConsumer();

            //TODO: expose WithProcessorConfig() method
            var processorConfig = Options.Create(new ProcessorConfig
            {
                Topic = _topic
            });

            return new KafkaProcessor<TKey, TValue>(
                consumer,
                _topicPartitionQueueSelector,
                processorConfig);
        }

        private IConsumer<TKey, TValue> BuildConsumer()
        {
            var builder = new ConsumerBuilder<TKey, TValue>(_consumerConfig)
                .SetPartitionsAssignedHandler(OnPartitionsAssigned)
                .SetPartitionsRevokedHandler(OnPartitionsRevoked);

            if (_keyDeserializer != null)
            {
                builder.SetKeyDeserializer(_keyDeserializer);
            }

            if (_valueDeserializer != null)
            {
                builder.SetValueDeserializer(_valueDeserializer);
            }

            return builder.Build();
        }

        private void OnPartitionsRevoked(
            IConsumer<TKey, TValue> consumer,
            List<TopicPartitionOffset> partitions)
        {
            _topicPartitionQueueSelector.Remove(partitions.Select(p => p.TopicPartition));

            try
            {
                consumer.Commit();
            }
            catch (KafkaException ex)
                when (ex.Error.Code == ErrorCode.Local_NoOffset)
            {
                // ignore
            }
        }

        private IEnumerable<TopicPartitionOffset> OnPartitionsAssigned(
            IConsumer<TKey, TValue> consumer,
            List<TopicPartition> partitions)
        {
            foreach (var partition in partitions)
            {
                var messageHandler = _handlerFactory.Invoke(partition);

                _topicPartitionQueueSelector.AddQueue(partition, messageHandler);
            }

            return partitions.Select(p => new TopicPartitionOffset(p, Offset.Stored));
        }

        private void CheckIfConfigured()
        {
            CheckIfConsumerConfigSet();
            CheckIfHandlerFactorySet();
            CheckIfTopicSet();
        }

        private void CheckIfConsumerConfigSet()
        {
            if (_consumerConfig == null) 
                throw new InvalidOperationException("'consumerConfig' must be set!");
        }

        private void CheckIfHandlerFactorySet()
        {
            if (_handlerFactory == null) 
                throw new InvalidOperationException("'handlerFactory' must be set!");
        }

        private void CheckIfTopicSet()
        {
            if (string.IsNullOrEmpty(_topic)) 
                throw new InvalidOperationException("'topic' must be set!");
        }
    }
}