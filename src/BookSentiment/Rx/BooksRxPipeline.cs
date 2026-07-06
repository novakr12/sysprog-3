using System.Reactive.Concurrency;
using System.Reactive.Linq;
using Akka.Actor;
using IScheduler = System.Reactive.Concurrency.IScheduler;
using BookSentiment.Models;
using BookSentiment.Services;

namespace BookSentiment.Rx;

// Periodicno poziva API i salje opise aktorima
public sealed class BooksRxPipeline : IDisposable
{
    private readonly GoogleBooksClient _client;
    private readonly IActorRef _coordinator;
    private readonly TimeSpan _interval;
    private readonly Action<string> _log;
    private readonly IScheduler _scheduler;
    private IDisposable? _subscription;

    public BooksRxPipeline(
        GoogleBooksClient client,
        IActorRef coordinator,
        TimeSpan interval,
        Action<string> log,
        IScheduler? scheduler = null)
    {
        _client = client;
        _coordinator = coordinator;
        _interval = interval;
        _log = log;
        _scheduler = scheduler ?? TaskPoolScheduler.Default;
    }

    public void Start()
    {
        _subscription = Observable
            .Timer(TimeSpan.Zero, _interval, _scheduler)                         // svakih _interval
            .SelectMany(_ => Observable.FromAsync(GetActiveQueriesAsync))         // uzmi pojmove
            .SelectMany(queries => queries)                                      // jedan po jedan
            .SelectMany(query => Observable.FromAsync(ct => FetchAsync(query, ct))) // pozovi API
            .SelectMany(batch => batch)                                          // jedna po jedna knjiga
            .Subscribe(
                onNext: msg => _coordinator.Tell(msg),                          // posalji aktorima
                onError: ex => _log($"[Rx] greska u toku: {ex.Message}"));
    }

    private async Task<IEnumerable<string>> GetActiveQueriesAsync(CancellationToken ct)
    {
        try
        {
            // Ask = cekaj odgovor
            var active = await _coordinator
                .Ask<Messages.ActiveQueries>(new Messages.GetActiveQueries(), TimeSpan.FromSeconds(5))
                .ConfigureAwait(false);
            return active.Queries;
        }
        catch (Exception ex)
        {
            _log($"[Rx] neuspesno citanje aktivnih pojmova: {ex.Message}");
            return Array.Empty<string>();
        }
    }

    private async Task<IEnumerable<Messages.BookDescriptionMessage>> FetchAsync(
        string query, CancellationToken ct)
    {
        try
        {
            var volumes = await _client.SearchAsync(query, maxResults: 20, ct).ConfigureAwait(false);

            // Izbaci bez opisa + pretvori u poruke
            var mapped = volumes
                .Where(v => !string.IsNullOrWhiteSpace(v.Description))
                .Select(v => new Messages.BookDescriptionMessage(
                    query,
                    v.Id,
                    v.Title,
                    v.Description!.Trim()))
                .ToList();

            _log($"[Rx] '{query}': preuzeto {volumes.Count} knjiga, {mapped.Count} sa opisom.");
            return mapped;
        }
        catch (Exception ex)
        {
            _log($"[Rx] neuspesno preuzimanje za '{query}': {ex.Message}");
            return Array.Empty<Messages.BookDescriptionMessage>();
        }
    }

    public void Dispose() => _subscription?.Dispose();
}
