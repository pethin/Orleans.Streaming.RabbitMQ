using System;

namespace IntegrationTests.Streams
{
    public class TestEvent : IEquatable<TestEvent>
    {
        internal TestEvent(int counter)
        {
            Counter = counter;
        }

        public int Counter { get; }

        public bool Equals(TestEvent? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Counter == other.Counter;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == GetType() && Equals((TestEvent) obj);
        }

        public override int GetHashCode()
        {
            return Counter;
        }
    }

    public static class TestEventFactory
    {
        private static int _internalCounter = 0;

        public static void Reset() => _internalCounter = 0;
        
        public static TestEvent Generate() => new TestEvent(_internalCounter++);
    }
}