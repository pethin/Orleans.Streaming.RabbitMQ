namespace IntegrationTests.Models
{
    public class TestResult<T> where T : class
    {
        public T? Expected { get; set; }
        public T? Actual { get; set; }
    }
}