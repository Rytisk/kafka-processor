using FluentAssertions;
using Moq;

namespace KafkaConsumer.Tests.Extensions
{
	public static class ObjectExtensions
	{
		public static bool IsEquivalentTo(this object actual, object expected)
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