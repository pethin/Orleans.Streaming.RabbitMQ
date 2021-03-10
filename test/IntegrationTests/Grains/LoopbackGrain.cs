using System;
using System.Linq;
using System.Threading.Tasks;
using IntegrationTests.Models;
using IntegrationTests.Streams;
using Orleans;
using Orleans.Concurrency;
using Orleans.Streams;

namespace IntegrationTests.Grains
{
    public interface ILoopbackGrain : IBaseTestGrain
    {
        Task<TestResult<TestEvent>?> SendTestEvent();
    }

    [Reentrant]
    [ImplicitStreamSubscription(nameof(LoopbackGrain))]
    public class LoopbackGrain : BaseTestGrain, ILoopbackGrain
    {
        private TestEvent? _model;
        private TaskCompletionSource<TestResult<TestEvent>>? _completion;

        public override async Task OnActivateAsync()
        {
            var provider = GetStreamProvider(Constants.RabbitMQStreamProvider);
            var stream = provider.GetStream<TestEvent>(this.GetPrimaryKey(), nameof(LoopbackGrain));
            
            _completion = new TaskCompletionSource<TestResult<TestEvent>>();
            _model = TestEventFactory.Generate();
            
            await stream.SubscribeAsync((actual, token) =>
            {
                _completion.SetResult(new TestResult<TestEvent>
                {
                    Actual = actual,
                    Expected = _model
                });

                return Task.CompletedTask;
            });
        }

        public async Task<TestResult<TestEvent>?> SendTestEvent()
        {
            var streamProvider = GetStreamProvider(Constants.RabbitMQStreamProvider);
            var stream = streamProvider.GetStream<TestEvent>(this.GetPrimaryKey(), nameof(LoopbackGrain));
            await stream.OnNextAsync(_model!);

            await Task.WhenAny(_completion!.Task, Task.Delay(2000));
            return _completion.Task.IsCompleted
                ? _completion.Task.Result
                : throw new TimeoutException();
        }
    }
}