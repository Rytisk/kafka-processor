using System;
using System.Collections.Generic;
using Confluent.Kafka;
using KafkaProcessor.MessageHandler;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaProcessor.Extensions
{
    internal class ScopedHandlerFactory<TKey, TValue>
    {
        private readonly IServiceProvider _serviceProvider;

        private List<IServiceScope> _scopes;
        
        public ScopedHandlerFactory(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _scopes = new List<IServiceScope>();
        }

        public IMessageHandler<TKey, TValue> Create<THandler>(TopicPartition topicPartition)
            where THandler : IMessageHandler<TKey, TValue>
        {
            var scope = _serviceProvider.CreateScope();

            _scopes.Add(scope);
            
            return scope.ServiceProvider.GetService<THandler>();
        }

        public void Close()
        {
            _scopes.ForEach(s => s.Dispose());
            _scopes.Clear();
        }
    }
}
