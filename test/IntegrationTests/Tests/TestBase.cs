using System.Threading.Tasks;
using IntegrationTests.Grains;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Logging.Debug;
using Orleans;
using Orleans.Hosting;
using Orleans.Streaming.RabbitMQ.Hosting;
using Orleans.TestingHost;
using Xunit;
using Xunit.Abstractions;

namespace IntegrationTests.Tests
{
    public class TestBase : IAsyncLifetime
    {
        private short _noOfSilos;

        protected TestCluster? Cluster { get; private set; }

        public static string HostName = "127.0.0.1";
        public static string UserName = "user";
        public static string Password = "cRn2vmcqkV";

        private TestClusterBuilder? _builder;

        protected void Initialize(short noOfSilos)
            => Initialize<ClientBuilderConfigurator, SiloBuilderConfigurator>(noOfSilos);

        protected void Initialize<TClientConfig, TSiloConfig>(short noOfSilos)
            where TClientConfig : IClientBuilderConfigurator, new()
            where TSiloConfig : IHostConfigurator, new()
        {
            _noOfSilos = noOfSilos;
            _builder = new TestClusterBuilder(_noOfSilos);
            _builder.AddSiloBuilderConfigurator<TSiloConfig>();
            _builder.AddClientBuilderConfigurator<TClientConfig>();
        }

        protected void ShutDown() => Cluster?.StopAllSilos();

        public virtual Task InitializeAsync()
        {
            Cluster = _builder?.Build();
            Cluster?.Deploy();

            return Task.CompletedTask;
        }

        public Task DisposeAsync()
        {
            ShutDown();
            return Task.CompletedTask;
        }
    }

    public class ClientBuilderConfigurator : IClientBuilderConfigurator
    {
        public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            => clientBuilder
                .AddRabbitMQStreams(Constants.RabbitMQStreamProvider, builder =>
                {
                    builder.Configure(options =>
                    {
                        options.HostName = TestBase.HostName;
                        options.UserName = TestBase.UserName;
                        options.Password = TestBase.Password;
                    });
                })
                .ConfigureApplicationParts(parts =>
                    parts.AddApplicationPart(typeof(LoopbackGrain).Assembly).WithReferences());
    }

    public class SiloBuilderConfigurator : IHostConfigurator
    {
        public void Configure(IHostBuilder hostBuilder)
            => hostBuilder
                .ConfigureLogging(builder =>
                {
                    builder.AddDebug();
                    builder.SetMinimumLevel(LogLevel.Trace);
                })
                .UseOrleans(builder =>
                {
                    builder
                        .AddMemoryGrainStorage("PubSubStore")
                        .AddRabbitMQStreams(Constants.RabbitMQStreamProvider, builder =>
                        {
                            builder.Configure(options =>
                            {
                                options.HostName = TestBase.HostName;
                                options.UserName = TestBase.UserName;
                                options.Password = TestBase.Password;
                            });
                        })
                        .ConfigureApplicationParts(parts =>
                            parts.AddApplicationPart(typeof(LoopbackGrain).Assembly).WithReferences());
                });
    }
}