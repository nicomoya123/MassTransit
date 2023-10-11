namespace MassTransit.KafkaIntegration.Tests
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Ductus.FluentDocker.Builders;
    using Ductus.FluentDocker.Extensions;
    using Ductus.FluentDocker.Services;
    using Ductus.FluentDocker.Services.Extensions;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Diagnostics.HealthChecks;
    using NUnit.Framework;
    using TestFramework;
    using Testing;

    public class HealthCheck_Specs :
        InMemoryTestFixture
    {
        const string Topic = "health-check";
        
        [Test]
        public async Task Should_be_healthy()
        {
            await using var provider = new ServiceCollection()
                .ConfigureKafkaTestOptions(options =>
                {
                    options.CreateTopicsIfNotExists = true;
                    options.TopicNames = new[] { Topic };
                })
                .AddMassTransitTestHarness(x =>
                {
                    x.AddRider(rider =>
                    {
                        rider.UsingKafka((_, k) =>
                        {
                            k.TopicEndpoint<Null, Ignore>(Topic, nameof(HealthCheck_Specs), _ =>
                            {
                            });
                        });
                    });
                }).BuildServiceProvider();
            var harness = provider.GetTestHarness();

            var healthCheckService = provider.GetRequiredService<HealthCheckService>();

            await healthCheckService.WaitForHealthStatus(HealthStatus.Unhealthy);

            await harness.Start();

            await healthCheckService.WaitForHealthStatus(HealthStatus.Healthy);
        }

        [Test]
        public async Task should_be_degraded_when_lost_connection()
        {
            await using var provider = new ServiceCollection()
                .ConfigureKafkaTestOptions(options =>
                {
                    options.CreateTopicsIfNotExists = true;
                    options.TopicNames = new[] { Topic };
                })
                .AddMassTransitTestHarness(x =>
                {
                    x.AddRider(rider =>
                    {
                        rider.UsingKafka((_, k) =>
                        {
                            k.TopicEndpoint<Null, Ignore>(Topic, nameof(HealthCheck_Specs), _ =>
                            {
                            });
                        });
                    });
                }).BuildServiceProvider();
            var harness = provider.GetTestHarness();

            var healthCheckService = provider.GetRequiredService<HealthCheckService>();

            await healthCheckService.WaitForHealthStatus(HealthStatus.Unhealthy);

            await harness.Start();

            KafkaHost._brokerContainerService.WaitForRunning();
            await healthCheckService.WaitForHealthStatus(HealthStatus.Healthy);

            KafkaHost._brokerContainerService.Stop();
            //KafkaHost._brokerContainerService.WaitForStopped();

            await healthCheckService.WaitForHealthStatus(HealthStatus.Degraded);

            CancellationTokenSource ct = new CancellationTokenSource(5000);
            int retires = 0;
            while (!ct.IsCancellationRequested)
            {
                var report = await healthCheckService.CheckHealthAsync(ct.Token);
                Assert.True(report.Status == HealthStatus.Degraded, "failed after: " + retires + " retries");
            }

        }
    }
}
