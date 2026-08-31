using Confluent.Kafka;
using Confluent.Kafka.Admin;

var bootstrap = Environment.GetEnvironmentVariable("CASCADE_BOOTSTRAP_SERVERS") ?? "127.0.0.1:19092";
var topic = args.Length > 0 ? args[0] : $"dotnet-compat-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
var expected = Enumerable.Range(0, 25).Select(index => $"dotnet-{index}").ToArray();

using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
{
    await admin.CreateTopicsAsync(
        new[] { new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 } },
        new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
}

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrap,
    EnableIdempotence = true,
    Acks = Acks.All,
    MessageTimeoutMs = 15_000
};
using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
{
    foreach (var value in expected)
    {
        var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = value });
        if (result.Status == PersistenceStatus.NotPersisted)
        {
            throw new InvalidOperationException($".NET producer did not persist {value}");
        }
    }
    producer.Flush(TimeSpan.FromSeconds(20));
}

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = $"{topic}-readers",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    SessionTimeoutMs = 10_000
};
var actual = new List<string>(expected.Length);
using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
{
    consumer.Subscribe(topic);
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
    while (actual.Count < expected.Length)
    {
        var record = consumer.Consume(timeout.Token);
        actual.Add(record.Message.Value);
    }
    consumer.Commit();
    consumer.Close();
}

if (!actual.SequenceEqual(expected))
{
    throw new InvalidOperationException(
        $".NET mismatch: expected={string.Join(',', expected)}, actual={string.Join(',', actual)}");
}
Console.WriteLine($"Confluent.Kafka .NET verified {actual.Count}/{expected.Length} records through {bootstrap}");
