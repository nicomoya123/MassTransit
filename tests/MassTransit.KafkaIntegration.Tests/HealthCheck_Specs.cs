namespace MassTransit.KafkaIntegration.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Ductus.FluentDocker.Builders;
    using Ductus.FluentDocker.Commands;
    using Ductus.FluentDocker.Executors;
    using Ductus.FluentDocker.Executors.Parsers;
    using Ductus.FluentDocker.Extensions;
    using Ductus.FluentDocker.Model.Common;
    using Ductus.FluentDocker.Model.Containers;
    using Ductus.FluentDocker.Services;
    using Ductus.FluentDocker.Services.Extensions;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Diagnostics.HealthChecks;
    using NUnit.Framework;
    using TestFramework;
    using TestFramework.Logging;
    using Testing;

    public class HealthCheck_Specs :
        InMemoryTestFixture
    {
        const string Topic = "health-check";

        [Test, Timeout(1000 * 30)]
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


            CancellationTokenSource ct1 = new CancellationTokenSource(1000 * 20);
            var id = KafkaHost.BrokerContainerService.Id;
            while (!ct1.IsCancellationRequested)
            {
                KafkaHost.BrokerContainerService.WaitForRunning();
                await healthCheckService.WaitForHealthStatus(HealthStatus.Healthy);
                TestContext.Out.WriteLine("Consumer Healthy");
                var res = KafkaHost.BrokerContainerService.DockerHost.Kill(id);
                Assert.True(res.Success);
                TestContext.Out.WriteLine("Broker Killed");
                await healthCheckService.WaitForHealthStatus(HealthStatus.Degraded);
                TestContext.Out.WriteLine("Consumer Degraded");
                var res1 = KafkaHost.BrokerContainerService.DockerHost.Start(id);
                Assert.True(res1.Success);
                TestContext.Out.WriteLine("Broker Started");
                KafkaHost.BrokerContainerService.WaitForRunning();
                TestContext.Out.WriteLine("Broker Running");


            }
            CancellationTokenSource ct = new CancellationTokenSource(1000 * 5);
            int retires = 0;
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var report = await healthCheckService.CheckHealthAsync(ct.Token);
                    Assert.True(report.Status == HealthStatus.Healthy, "failed after: " + retires + " retries");
                }
            }
            catch(TaskCanceledException ex)
            {

            }

        }
    }


    static class DockerExt
    {
        internal static string RenderBaseArgs(this DockerUri host, ICertificatePaths certificates = null)
        {
            var args = string.Empty;
            if (null != host && !host.IsStandardDaemon)
            {
                args = $" -H {host}";
            }

            if (null == certificates)
                return args;

            args +=
                $" --tlsverify --tlscacert={certificates.CaCertificate} --tlscert={certificates.ClientCertificate} --tlskey={certificates.ClientKey}";

            return args;
        }

        public static CommandResponse<string> Kill(this DockerUri host, string id, TimeSpan? killTimeout = null,
            ICertificatePaths certificates = null)
        {
            var arg = $"{host.RenderBaseArgs(certificates)} kill";
            if (null != killTimeout)
            {
                arg += $" --time={Math.Round(killTimeout.Value.TotalSeconds, 0)}";
            }

            arg += $" {id}";

            return new ProcessExecutor<SingleStringResponseParser, string>(
                "docker".ResolveBinary(),
                arg).Execute();
        }
    }
}
