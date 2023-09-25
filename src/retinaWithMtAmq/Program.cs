using System;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using MassTransit;
using MassTransit.Transports;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using retinaWithMtAmq;
using Serilog;
using Serilog.Events;

IConfiguration configuration = new ConfigurationBuilder().AddUserSecrets<Program>().Build();
var retinaDedicated = configuration.GetSection("retina_dedicated")!;
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();


var services = new ServiceCollection()
    .AddLogging(configuration => configuration.AddSerilog(Log.Logger))
    //.AddMassTransit(x =>
    .ConfigureKafkaTestOptions(options =>
    {
        options.CreateTopicsIfNotExists = true;
        options.TopicNames = new[] { "msk.nscm.playground-dev.topic.internal.dedicated.v1" };
    }).AddMassTransitTestHarness(x =>
    {
        x.UsingInMemory();
        x.AddConsumer<Consumer1>();
        x.AddRider(rider =>
        {
            rider.AddProducer<Class1>("msk.nscm.playground-dev.topic.internal.dedicated.v1");
            rider.AddConsumer<Consumer1>();
            rider.UsingKafka((context, k) =>
            {
                //k.Host(retinaDedicated["Host"], kafkaConfig =>
                //{
                //    kafkaConfig.UseSasl(sasl =>
                //    {
                //        sasl.Username = retinaDedicated["SaslUsername"];
                //        sasl.Password = retinaDedicated["SaslPassword"];
                //        sasl.Mechanism = SaslMechanism.ScramSha512;
                //        sasl.SecurityProtocol = SecurityProtocol.SaslSsl;
                //    });
                //    kafkaConfig.UseSsl(ssl =>
                //    {
                //        ssl.SslCaPem = Encoding.UTF8.GetString(Convert.FromBase64String(retinaDedicated["SslCaCert"]));
                //        ssl.KeyPassword = retinaDedicated["SslKeyPassword"];
                //    });
                //});

                k.Host("localhost:9092");
                k.ClientId = $"local-{Guid.NewGuid()}";

                k.TopicEndpoint<Class1>("msk.nscm.playground-dev.topic.internal.dedicated.v1",
                    "msk.nscm.playground.consumergroup.v1", c =>
                {
                    c.GroupInstanceId = "local";
                    c.AutoOffsetReset = AutoOffsetReset.Earliest;
                    c.ConfigureConsumer<Consumer1>(context);
                    c.CreateIfMissing();
                });
            });
        });

    });

await using var provider = services.BuildServiceProvider(true);
var busControl = provider.GetRequiredService<IBusControl>();
await busControl.StartAsync();

Console.WriteLine("Hello, World!");
while (true)
{
    var line= Console.ReadLine(); 
    await using var scope = provider.CreateAsyncScope();
    var publishEndpoint = scope.ServiceProvider.GetRequiredService<ITopicProducer<Class1>>();
    await publishEndpoint.Produce(new Class1(line!));
    Console.WriteLine($"Sent message: {line}");
}



public class Consumer1 : IConsumer<Class1>
{
    public async Task Consume(ConsumeContext<Class1> context)
    {
        Console.WriteLine($"received: {context.Message.Name}, offset: {context.Offset()}");
        //throw new ArgumentException(context.Message.Name);
        await context.ConsumeCompleted;
        //return Task.CompletedTask;

    }
}
