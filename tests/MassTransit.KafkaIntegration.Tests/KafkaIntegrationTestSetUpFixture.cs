using NUnit.Framework;

[assembly: Parallelizable(ParallelScope.None)]
[assembly: LevelOfParallelism(1)]


namespace MassTransit.KafkaIntegration.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Confluent.SchemaRegistry;
    using Ductus.FluentDocker.Builders;
    using Ductus.FluentDocker.Extensions;
    using Ductus.FluentDocker.Services;
    using RetryPolicies;


    public static class KafkaHost
    {
        public static readonly ICompositeService DockerServices;
        public static readonly IContainerService BrokerContainerService;
        static KafkaHost()
        {
            DockerServices = new Builder()
                .UseContainer().UseCompose().FromFile("./docker-compose.yml")
                .Build()
                .Start();

            BrokerContainerService = DockerServices.Containers.First(c => c.Name == "broker");
            BrokerContainerService.WaitForRunning();
        }

        public static bool IsRunning()
        {
            return BrokerContainerService.State == ServiceRunningState.Running;
        }
    }

    [SetUpFixture]
    public class KafkaIntegrationTestSetUpFixture
    {
        [OneTimeSetUp]
        public async Task Before_any()
        {
            KafkaHost.IsRunning();
            await CheckBrokerReady();
        }

        static Task CheckBrokerReady()
        {
            return Retry.Interval(10, 5000).Retry(async () =>
            {
                using var client = new CachedSchemaRegistryClient(new Dictionary<string, string>
                {
                    { "schema.registry.url", "localhost:8081" },
                });

                await client.GetAllSubjectsAsync();

                var clientConfig = new ClientConfig { BootstrapServers = "localhost:9092" };
                using var adminClient = new AdminClientBuilder(clientConfig).Build();

                adminClient.GetMetadata(TimeSpan.FromSeconds(60));
            });
        }
    }
}
