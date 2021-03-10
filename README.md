# RabbitMQ Provider for Orleans Streaming

Usage
-----
```cs
public class SiloBuilderConfigurator : IHostConfigurator
{
    public void Configure(IHostBuilder hostBuilder)
        => hostBuilder
            .UseOrleans(builder =>
            {
                builder
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddRabbitMQStreams("RabbitMQ", builder =>
                    {
                        builder.Configure(options =>
                        {
                            options.HostName = TestBase.HostName;
                            options.UserName = TestBase.UserName;
                            options.Password = TestBase.Password;
                        });
                    });
            });
}

public class ClientBuilderConfigurator : IClientBuilderConfigurator
{
    public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        => clientBuilder
            .AddRabbitMQStreams("RabbitMQ", builder =>
            {
                builder.Configure(options =>
                {
                    options.HostName = TestBase.HostName;
                    options.UserName = TestBase.UserName;
                    options.Password = TestBase.Password;
                });
            });
}
```