using System.Collections.Generic;

namespace s3tests.Data
{
	public class MyEqualException : Xunit.Sdk.XunitException
	{
		public string UserMessage { get; }

		public MyEqualException(object expected, object actual, string userMessage)
			: base($"{userMessage}\nExpected: {expected}\nActual: {actual}")
		{
			UserMessage = userMessage;
		}
	}

	public static class AssertX
	{
		public static void Equal<T>(T expected, T actual, string userMessage)
		{
			bool areEqual;

			if (expected == null && actual == null)
				areEqual = true;
			else if (expected == null || actual == null)
				areEqual = false;
			else
				areEqual = EqualityComparer<T>.Default.Equals(expected, actual);

			if (!areEqual)
				throw new MyEqualException(expected, actual, userMessage);
		}
	}
}
