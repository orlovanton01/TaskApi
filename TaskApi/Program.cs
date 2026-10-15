using Microsoft.EntityFrameworkCore;
using System.Text.Json.Serialization;
using TaskApi;

var builder = WebApplication.CreateBuilder(args);

// Настройка JSON: Игнорируем регистр свойств и разрешаем парсить Enum из строк/чисел.
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNameCaseInsensitive = true;
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter(allowIntegerValues: true));
});

// Регистрируем PostgreSQL только если это не тестовое окружение.
if (!builder.Environment.IsEnvironment("Testing"))
{
    builder.Services.AddDbContext<TaskDbContext>(options =>
        options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));
}

builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddSingleton<IRabbitMqPublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
builder.Services.AddHostedService<RabbitMqPublisher>();

var app = builder.Build();

// Для упрощения использую EnsureCreated вместо миграций EF Core.
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
    db.Database.EnsureCreated();
}

app.MapPost("/tasks", async (CreateTaskDto dto, TaskDbContext db) =>
{
    // Валидация Title (не пустой, не более 200 символов).
    if (string.IsNullOrWhiteSpace(dto.Title) || dto.Title.Length > 200)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            { "Title", new[] { "Title must not be empty or whitespace, and must be <= 200 characters." } }
        });
    }

    var task = new TaskItem
    {
        Id = Guid.NewGuid(),
        Title = dto.Title.Trim(),
        IsCompleted = false,
        Priority = dto.Priority ?? Priority.Medium,
        CreatedAt = DateTimeOffset.UtcNow
    };

    db.Tasks.Add(task);
    await db.SaveChangesAsync();

    return Results.Created($"/tasks/{task.Id}", task);
});

app.MapGet("/tasks", async (TaskDbContext db) =>
{
    var tasks = await db.Tasks.ToListAsync();
    return Results.Ok(tasks);
});

app.MapPut("/tasks/{id:guid}/complete", async (Guid id, TaskDbContext db, IRabbitMqPublisher publisher) =>
{
    var task = await db.Tasks.FindAsync(id);
    if (task == null) return Results.NotFound();

    // Если задача уже завершена (последовательный повторный запрос).
    if (task.IsCompleted) return Results.Conflict("Task is already completed.");

    task.IsCompleted = true;
    task.CompletedAt = DateTimeOffset.UtcNow;

    try
    {
        await db.SaveChangesAsync();
    }
    catch (DbUpdateConcurrencyException)
    {
        // Конкурентный запрос (Race condition) - xmin изменился.
        return Results.Conflict("Task was modified or completed by another request.");
    }

    // Публикуем только после успешного сохранения в БД.
    publisher.EnqueueTaskCompleted(task);

    return Results.Ok(task);
});

app.MapDelete("/tasks/{id:guid}", async (Guid id, TaskDbContext db) =>
{
    var task = await db.Tasks.FindAsync(id);
    if (task == null) return Results.NotFound();

    db.Tasks.Remove(task);
    await db.SaveChangesAsync();

    return Results.NoContent();
});

app.Run();

public record CreateTaskDto(string Title, Priority? Priority);

// Необходимо для WebApplicationFactory в интеграционных тестах.
public partial class Program { }