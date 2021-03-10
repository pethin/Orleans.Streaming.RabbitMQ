using System.Threading.Tasks;
using Orleans;

namespace IntegrationTests.Grains
{
    public interface IBaseTestGrain : IGrainWithGuidKey
    {
        Task Initialize();
    }
    
    public class BaseTestGrain : Grain, IBaseTestGrain
    {
        public Task Initialize() => Task.CompletedTask;
    }
}