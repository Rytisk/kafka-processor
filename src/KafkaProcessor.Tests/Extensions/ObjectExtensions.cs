using FluentAssertions;
using Moq;

namespace KafkaProcessor.Tests.Extensions
{
    public static class ObjectExtensions
    {
        public static bool IsEquivalentTo<T>(this T actual, T expected)
        {
            actual.Should().BeEquivalentTo(expected);

            return true;
        }

        public static T IsActual<T>(this T expected)
        {
            return It.Is<T>(actual => actual.IsEquivalentTo(expected));
        }
    }
}