namespace TaskApi
{
    using System.Text;
    using System.Text.Json;
    using System.Threading.Channels;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using RabbitMQ.Client;

    public interface IRabbitMqPublisher
    {
        void EnqueueTaskCompleted(TaskItem task);
    }

    public class RabbitMqPublisher : IRabbitMqPublisher, IHostedService, IAsyncDisposable
    {
        private readonly ILogger<RabbitMqPublisher> _logger;
        private readonly IConfiguration _config;
        private IConnection? _connection;
        private IModel? _channel;
        private readonly Channel<TaskItem> _queue;
        private Task? _backgroundTask;
        private readonly CancellationTokenSource _stoppingCts = new();

        public RabbitMqPublisher(ILogger<RabbitMqPublisher> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
            // SingleReader = true оптимизирует чтение из очереди одним фоновым потоком.
            _queue = Channel.CreateUnbounded<TaskItem>(new UnboundedChannelOptions { SingleReader = true });
        }

        public void EnqueueTaskCompleted(TaskItem task)
        {
            if (!_queue.Writer.TryWrite(task))
            {
                _logger.LogWarning("Failed to enqueue task {TaskId} to RabbitMQ queue.", task.Id);
            }
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = _config["RabbitMq:HostName"] ?? "localhost",
                    Port = int.Parse(_config["RabbitMq:Port"] ?? "5672"),
                    UserName = _config["RabbitMq:UserName"] ?? "guest",
                    Password = _config["RabbitMq:Password"] ?? "guest"
                };
                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();
                _channel.ExchangeDeclare("task.events", ExchangeType.Direct, durable: true);
            }
            catch (Exception ex)
            {
                // Если RabbitMQ недоступен - логируем, но не падаем. Задачи будут завершаться.
                _logger.LogError(ex, "Failed to connect to RabbitMQ on startup. Publishing will fail silently.");
            }

            _backgroundTask = Task.Run(() => ProcessQueueAsync(_stoppingCts.Token));
            return Task.CompletedTask;
        }

        private async Task ProcessQueueAsync(CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var task in _queue.Reader.ReadAllAsync(cancellationToken))
                {
                    try
                    {
                        if (_channel == null || _channel.IsClosed)
                        {
                            _logger.LogWarning("RabbitMQ channel is closed. Message for task {TaskId} dropped.", task.Id);
                            continue;
                        }

                        var message = new
                        {
                            taskId = task.Id,
                            title = task.Title,
                            completedAt = task.CompletedAt,
                            priority = task.Priority.ToString()
                        };
                        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

                        // Использую RabbitMQ.Client 6.x (синхронный BasicPublish), 
                        // так как в 7.x API стал полностью асинхронным, а время на тест ограничено.
                        // Это происходит в фоне, поэтому основной поток API не блокируется.
                        _channel.BasicPublish(
                            exchange: "task.events",
                            routingKey: "task.completed",
                            basicProperties: null,
                            body: body);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to publish RabbitMQ message for task {TaskId}", task.Id);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ожидаемое исключение при принудительной остановке.
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            // Сигнализируем, что новых задач не будет.
            _queue.Writer.TryComplete();

            if (_backgroundTask != null)
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                // Таймаут 5 секунд на отправку "висящих" сообщений.
                cts.CancelAfter(TimeSpan.FromSeconds(5));

                try
                {
                    await _backgroundTask.WaitAsync(cts.Token);
                }
                catch (OperationCanceledException)
                {
                    // Ожидаемое поведение при таймауте или отмене.
                }
            }

            try
            {
                // Проверяем IsOpen перед закрытием, чтобы избежать ошибок на уже закрытых соединениях.
                if (_channel?.IsOpen == true) _channel.Close();
                if (_connection?.IsOpen == true) _connection.Close();
            }
            catch (ObjectDisposedException)
            {
                // Игнорируем: объект уже удален (частая ситуация в тестах или при быстром рестарте).
            }
            catch (Exception ex)
            {
                // Защищаем сам вызов логгера, так как он может быть уже удален при teardown тестов.
                try
                {
                    _logger.LogError(ex, "Error closing RabbitMQ connection during shutdown.");
                }
                catch
                {
                    // Полностью игнорируем ошибки логгера на этапе shutdown.
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            _stoppingCts.Dispose();
            _channel?.Dispose();
            _connection?.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
