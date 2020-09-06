using FluentAssertions;
using Moq;

namespace KafkaConsumer.Tests.Extensions
{
	public static class ObjectExtensions
	{
		public static bool IsEquivalentTo(this object obj, object other)
		{
			obj.Should().BeEquivalentTo(other);

			return true;
		}

		public static T IsExpected<T>(this T obj)
		{
			return It.Is<T>(other => obj.IsEquivalentTo(other));
		}
	}
}