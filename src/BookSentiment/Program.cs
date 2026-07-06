using System.Reactive.Concurrency;
using Akka.Actor;
using Akka.Configuration;
using BookSentiment.Actors;
using BookSentiment.Models;
using BookSentiment.Rx;
using BookSentiment.Services;

// Log sa vremenom
void Log(string message) =>
    Console.WriteLine($"{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff} {message}");

// Prevod labele za prikaz
static string Srpski(string sentiment) =>
    sentiment == "Positive" ? "Pozitivan" : "Negativan";

// Web builder
var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.WebHost.UseUrls("http://localhost:5000");

// Fajl sa kljucem
builder.Configuration.AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: true);

// 1) Treniraj ML model
Log("[Start] Treniranje ML.NET modela za analizu sentimenta...");
var dataPath = Path.Combine(AppContext.BaseDirectory, "Data", "sentiment-data.tsv");
var sentimentModel = SentimentModel.Train(dataPath);
Log("[Start] Model za sentiment istreniran.");

// 2) Akka: poseban pul niti za sentiment radnike
var hocon = ConfigurationFactory.ParseString("""
    akka {
        loglevel = INFO
        stdout-loglevel = WARNING
        actor.debug.unhandled = on
    }

    sentiment-dispatcher {
        type = Dispatcher
        executor = "thread-pool-executor"
        thread-pool-executor {
            fixed-pool-size = 8
        }
        throughput = 1
    }
    """);

// Aktorski sistem + koordinator
using var actorSystem = ActorSystem.Create("BookSentimentSystem", hocon);
var coordinator = actorSystem.ActorOf(BooksCoordinatorActor.Props(sentimentModel), "coordinator");
Log("[Start] Aktorski sistem pokrenut (koordinator spreman).");

// 3) Kljuc: fajl, pa env varijabla
var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(20) };
var apiKey = builder.Configuration["GoogleBooks:ApiKey"]
    ?? Environment.GetEnvironmentVariable("GOOGLE_BOOKS_API_KEY");
if (string.IsNullOrWhiteSpace(apiKey))
    Log("[Start] Google Books API kljuc nije podesen — anoniman pristup (nizak limit, moguce HTTP 429).");
else
    Log("[Start] Koristi se podeseni Google Books API kljuc.");
// 4) Pokreni Rx tok
var booksClient = new GoogleBooksClient(httpClient, apiKey);
var pipeline = new BooksRxPipeline(
    booksClient,
    coordinator,
    interval: TimeSpan.FromSeconds(15),
    log: Log,
    scheduler: TaskPoolScheduler.Default);
pipeline.Start();
Log("[Start] Rx.NET tok pokrenut (poziva API na svakih 15s).");

var askTimeout = TimeSpan.FromSeconds(10);

// Offline test (bez API-ja)
if (args.Contains("--selftest"))
{
    const string q = "selftest";
    await coordinator.Ask<Messages.SearchRegistered>(new Messages.RegisterSearchQuery(q), askTimeout);

    var samples = new (string Id, string Title, string Desc)[]
    {
        ("1", "A Wonderful Tale", "A beautiful and deeply moving story that I could not put down."),
        ("2", "A Dreadful Bore", "A terrible, boring book that wasted my time completely."),
        ("3", "Joyful Journey", "An inspiring and uplifting journey that left me smiling."),
        ("4", "The Tedious Slog", "Dull, lifeless and painfully slow from beginning to end."),
    };
    foreach (var s in samples)
        coordinator.Tell(new Messages.BookDescriptionMessage(q, s.Id, s.Title, s.Desc));

    await Task.Delay(1500);
    var results = await coordinator.Ask<Messages.QueryResults>(new Messages.GetResults(q), askTimeout);
    Log($"[Test] analizirano: {results.TotalBooks} — pozitivnih: {results.Positive}, negativnih: {results.Negative}:");
    foreach (var b in results.Books.OrderBy(b => b.BookId))
        Log($"[Test]   '{b.Title}' -> {Srpski(b.Sentiment)} ({b.Confidence:P1})");

    pipeline.Dispose();
    httpClient.Dispose();
    await actorSystem.Terminate();
    return;
}

var app = builder.Build();

app.UseStaticFiles();   // poslužuje wwwroot/index.html

// Loguj svaki HTTP zahtev
app.Use(async (context, next) =>
{
    var req = context.Request;
    Log($"[HTTP] --> primljen zahtev {req.Method} {req.Path}{req.QueryString}");
    try
    {
        await next();
        Log($"[HTTP] <-- {req.Method} {req.Path} odgovor {context.Response.StatusCode}");
    }
    catch (Exception ex)
    {
        Log($"[HTTP] !!! {req.Method} {req.Path} greska: {ex.Message}");
        throw;
    }
});

// Registruj pojam
app.MapPost("/api/search", async (string q) =>
{
    if (string.IsNullOrWhiteSpace(q))
        return Results.BadRequest(new { error = "Parametar 'q' je obavezan." });

    var ack = await coordinator.Ask<Messages.SearchRegistered>(
        new Messages.RegisterSearchQuery(q), askTimeout);

    Log($"[HTTP] Registrovan pojam za pretragu '{ack.Query}'.");
    return Results.Ok(new
    {
        message = $"Pojam '{ack.Query}' se sada prati. Rezultate periodicno prikuplja Rx tok.",
        query = ack.Query
    });
});

// Vrati rezultate za pojam
app.MapGet("/api/results", async (string q) =>
{
    if (string.IsNullOrWhiteSpace(q))
        return Results.BadRequest(new { error = "Parametar 'q' je obavezan." });

    var reply = await coordinator.Ask<object>(new Messages.GetResults(q), askTimeout);
    return reply switch
    {
        Messages.QueryResults r => Results.Ok(r),
        Messages.QueryNotFound => Results.NotFound(new
        {
            error = $"Pojam '{q}' se ne prati. Prvo pozovite POST /api/search?q={q}."
        }),
        _ => Results.StatusCode(500)
    };
});

// Vrati listu pojmova
app.MapGet("/api/queries", async () =>
{
    var active = await coordinator.Ask<Messages.ActiveQueries>(
        new Messages.GetActiveQueries(), askTimeout);
    return Results.Ok(active.Queries);
});

Log("[Start] Web server slusa na http://localhost:5000");
Log("[Start] Otvorite http://localhost:5000 u pregledacu ili koristite REST API na /api.");

// Ciscenje pri gasenju
app.Lifetime.ApplicationStopping.Register(() =>
{
    Log("[Gasenje] Zaustavljanje Rx toka i aktorskog sistema...");
    pipeline.Dispose();
    httpClient.Dispose();
});

app.Run();
