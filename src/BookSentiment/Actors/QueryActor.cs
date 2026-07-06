using Akka.Actor;
using Akka.Event;
using Akka.Routing;
using BookSentiment.Models;
using BookSentiment.Services;

namespace BookSentiment.Actors;

// Cuva stanje za jedan pojam
public sealed class QueryActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly string _query;
    private readonly IActorRef _sentimentPool;

    private readonly Dictionary<string, BookResult> _books = new();                       // gotove
    private readonly Dictionary<string, (string Title, string Description)> _pending = new(); // cekaju
    private DateTimeOffset _lastUpdated = DateTimeOffset.UtcNow;

    public QueryActor(string query, SentimentModel model)
    {
        _query = query;

        // Pul radnika za paralelnu analizu
        _sentimentPool = Context.ActorOf(
            SentimentActor.Props(model)
                .WithRouter(new RoundRobinPool(Environment.ProcessorCount))
                .WithDispatcher("sentiment-dispatcher"),
            "sentiment");

        Receive<Messages.BookDescriptionMessage>(Handle);
        Receive<Messages.SentimentComputed>(Handle);
        Receive<Messages.GetResults>(_ => Sender.Tell(BuildResults()));
    }

    private void Handle(Messages.BookDescriptionMessage msg)
    {
        // Preskoci ako je vec obradjena ili ceka
        if (_books.ContainsKey(msg.BookId) || _pending.ContainsKey(msg.BookId))
            return;

        _pending[msg.BookId] = (msg.Title, msg.Description);
        _sentimentPool.Tell(new Messages.AnalyzeSentiment(_query, msg.BookId, msg.Description));
        _log.Info("Pojam '{0}': '{1}' poslat na analizu sentimenta.", _query, msg.Title);
    }

    private void Handle(Messages.SentimentComputed msg)
    {
        // Prebaci iz "ceka" u "gotovo"
        if (!_pending.Remove(msg.BookId, out var info))
            return;

        _books[msg.BookId] = new BookResult(
            msg.BookId,
            info.Title,
            info.Description,
            msg.Label,
            msg.Confidence,
            msg.Score,
            DateTimeOffset.UtcNow);

        _lastUpdated = DateTimeOffset.UtcNow;
        var label = msg.Label == "Positive" ? "Pozitivan" : "Negativan";
        _log.Info("Pojam '{0}': '{1}' -> {2} ({3:P1}). Ukupno analizirano: {4}.",
            _query, info.Title, label, msg.Confidence, _books.Count);
    }

    // Sastavi rezultate
    private Messages.QueryResults BuildResults()
    {
        var books = _books.Values
            .OrderByDescending(b => b.AnalysedUtc)
            .ToList();

        var positive = books.Count(b => b.Sentiment == "Positive");
        var avgConfidence = books.Count > 0 ? books.Average(b => b.Confidence) : 0d;

        return new Messages.QueryResults(
            _query,
            books.Count,
            positive,
            books.Count - positive,
            avgConfidence,
            books,
            _lastUpdated);
    }

    public static Props Props(string query, SentimentModel model) =>
        Akka.Actor.Props.Create(() => new QueryActor(query, model));
}
