namespace Tests
{
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.Mvc.Testing;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using System.Net.Http.Json;
    using System.Text.Json;
    using System.Text.Json.Serialization;
    using TaskApi;
    using Xunit;

    public class CompleteTaskIntegrationTest : IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly HttpClient _client;
        private readonly MockRabbitMqPublisher _mockPublisher;

        // Константное имя БД гарантирует, что все запросы теста работают с одной и той же памятью.
        private const string TestDbName = "IntegrationTestDb";

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            Converters = { new JsonStringEnumConverter(allowIntegerValues: true) }
        };

        public CompleteTaskIntegrationTest(WebApplicationFactory<Program> factory)
        {
            var testFactory = factory.WithWebHostBuilder(builder =>
            {
                builder.UseEnvironment("Testing");

                builder.ConfigureServices(services =>
                {
                    services.AddDbContext<TaskDbContext>(options =>
                        options.UseInMemoryDatabase(TestDbName));

                    // 1. Прицельно удаляем регистрацию реального класса как IHostedService.
                    var hostedServiceDescriptor = services.FirstOrDefault(d => d.ImplementationType == typeof(RabbitMqPublisher));
                    if (hostedServiceDescriptor != null)
                    {
                        services.Remove(hostedServiceDescriptor);
                    }

                    // 2. На всякий случай удаляем все регистрации интерфейса.
                    services.RemoveAll<IRabbitMqPublisher>();

                    // 3. Добавляем наш Мок.
                    services.AddSingleton<IRabbitMqPublisher, MockRabbitMqPublisher>();
                });
            });

            _client = testFactory.CreateClient();
            _mockPublisher = (MockRabbitMqPublisher)testFactory.Services.GetRequiredService<IRabbitMqPublisher>();
        }

        [Fact]
        public async Task CompleteTask_ShouldPublishToRabbitMq()
        {
            // 1. Создаем задачу.
            var createResponse = await _client.PostAsJsonAsync("/tasks", new { Title = "Buy milk", Priority = "High" });
            createResponse.EnsureSuccessStatusCode();

            var createdTask = await createResponse.Content.ReadFromJsonAsync<TaskItem>(JsonOptions);
            Assert.NotNull(createdTask);
            Assert.NotEqual(Guid.Empty, createdTask.Id);

            // 2. Проверка: Читаем все задачи, чтобы убедиться, что наша задача реально попала в БД.
            var getAllResponse = await _client.GetAsync("/tasks");
            getAllResponse.EnsureSuccessStatusCode();
            var allTasks = await getAllResponse.Content.ReadFromJsonAsync<List<TaskItem>>(JsonOptions);

            Assert.NotNull(allTasks);
            Assert.Contains(allTasks, t => t.Id == createdTask.Id);

            // 3. Завершаем задачу (теперь мы на 100% уверены, что ID существует в этой БД).
            var completeResponse = await _client.PutAsync($"/tasks/{createdTask.Id}/complete", null);
            completeResponse.EnsureSuccessStatusCode();

            // 4. Проверяем, что мок получил сообщение.
            var publishedTask = Assert.Single(_mockPublisher.PublishedTasks, t => t.Id == createdTask.Id);
            Assert.True(publishedTask.IsCompleted);
            Assert.NotNull(publishedTask.CompletedAt);
        }
    }

    public class MockRabbitMqPublisher : IRabbitMqPublisher
    {
        public List<TaskItem> PublishedTasks { get; } = new();

        public void EnqueueTaskCompleted(TaskItem task)
        {
            PublishedTasks.Add(task);
        }
    }
}
