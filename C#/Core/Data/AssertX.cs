namespace s3tests
{
	public class MyEqualException : Xunit.Sdk.EqualException
	{
		public MyEqualException(object expected, object actual, string userMessage)
			: base(expected, actual) { UserMessage = userMessage; }

		public override string Message => UserMessage + "\n" + base.Message;
	}

	public static class AssertX
	{
		public static void Equal<T>(T expected, T actual, string userMessage)
		{
			bool areEqual;

			if (expected == null || actual == null) areEqual = (expected == null && actual == null);
			else areEqual = expected.Equals(actual);

			if (!areEqual)
				throw new MyEqualException(expected, actual, userMessage);
		}
	}
}
