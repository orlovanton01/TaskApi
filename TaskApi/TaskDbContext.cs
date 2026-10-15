namespace TaskApi
{
    using Microsoft.EntityFrameworkCore;

    public class TaskDbContext : DbContext
    {
        public TaskDbContext(DbContextOptions<TaskDbContext> options) : base(options) { }

        public DbSet<TaskItem> Tasks => Set<TaskItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TaskItem>(b =>
            {
                b.HasKey(e => e.Id);
                b.Property(e => e.Title).HasMaxLength(200).IsRequired();
                b.Property(e => e.Priority).HasConversion<string>();

                // Явно указываем, что это токен конкурентности.
                // Для PostgreSQL это будет xmin, для InMemory - внутренняя версия строки.
                b.Property(e => e.RowVersion).IsRowVersion();
            });
        }
    }
}
