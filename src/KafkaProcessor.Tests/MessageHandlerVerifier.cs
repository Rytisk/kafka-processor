using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using KafkaProcessor.MessageHandler;
using Moq;
using Xunit;

namespace KafkaProcessor.Tests
{
    public class MessageHandlerVerifier
    {
        private readonly List<Message<string, string>> _messages;
        
        private int _index;

        public MessageHandlerVerifier(List<Message<string, string>> messages)
        {
            _messages = messages;
        }

        public static void Verify(
            Mock<IMessageHandler<string, string>> messageHandler,
            List<Message<string, string>> messages)
        {
            if(!messages.Any())
            {
                messageHandler.Verify(
                    mh => mh.HandleAsync(It.IsAny<Message<string, string>>()),
                    Times.Never());

                return;
            }

            var collectionVerifier = new MessageHandlerVerifier(messages);

            messageHandler.Verify(mh => mh.HandleAsync(
                It.Is<Message<string, string>>(msg => collectionVerifier.Validate(msg))));
            
            messageHandler.VerifyNoOtherCalls();

            Assert.Equal(messages.Count, collectionVerifier._index);
        }


        private bool Validate(Message<string, string> message)
        {
            _messages[_index].Should().BeEquivalentTo(message);

            _index++;

            return true;
        }
    }
}