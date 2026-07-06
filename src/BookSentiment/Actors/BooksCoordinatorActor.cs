using Akka.Actor;
using Akka.Event;
using BookSentiment.Models;
using BookSentiment.Services;

namespace BookSentiment.Actors;

// Glavni aktor: drzi po jedan QueryActor za svaki pojam
public sealed class BooksCoordinatorActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly SentimentModel _model;
    private readonly Dictionary<string, IActorRef> _queries = new(StringComparer.OrdinalIgnoreCase); // pojam -> aktor

    public BooksCoordinatorActor(SentimentModel model)
    {
        _model = model;

        Receive<Messages.RegisterSearchQuery>(Handle);
        Receive<Messages.BookDescriptionMessage>(Handle);
        Receive<Messages.GetResults>(Handle);
        Receive<Messages.GetActiveQueries>(_ =>
            Sender.Tell(new Messages.ActiveQueries(_queries.Keys.ToList())));
    }

    private void Handle(Messages.RegisterSearchQuery msg)
    {
        var query = msg.Query.Trim();
        if (!_queries.ContainsKey(query))
        {
            // Nov pojam => napravi aktora
            var child = Context.ActorOf(QueryActor.Props(query, _model), ChildName(query));
            _queries[query] = child;
            _log.Info("Registrovan novi pojam '{0}'. Trenutno se prati {1} pojmova.", query, _queries.Count);
        }

        Sender.Tell(new Messages.SearchRegistered(query));
    }

    private void Handle(Messages.BookDescriptionMessage msg)
    {
        // Prosledi opis pravom aktoru
        if (_queries.TryGetValue(msg.Query, out var child))
            child.Forward(msg);
        else
            _log.Warning("Odbacen opis knjige za pojam koji se ne prati '{0}'.", msg.Query);
    }

    private void Handle(Messages.GetResults msg)
    {
        // Prosledi zahtev ili vrati "ne prati se"
        if (_queries.TryGetValue(msg.Query, out var child))
            child.Forward(msg);
        else
            Sender.Tell(new Messages.QueryNotFound(msg.Query));
    }

    // Bezbedno ime aktora
    private static string ChildName(string query)
    {
        var safe = new string(query.Select(c => char.IsLetterOrDigit(c) ? c : '-').ToArray());
        return $"query-{safe}-{Math.Abs(query.GetHashCode())}";
    }

    public static Props Props(SentimentModel model) =>
        Akka.Actor.Props.Create(() => new BooksCoordinatorActor(model));
}
