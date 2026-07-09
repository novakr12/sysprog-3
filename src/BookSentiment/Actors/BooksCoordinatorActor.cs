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
        Receive<Terminated>(Handle);
    }

    // Nadzor nad potomcima (Let it crash): ako QueryActor padne, restartuj ga
    protected override SupervisorStrategy SupervisorStrategy() =>
        new OneForOneStrategy(
            3,
            TimeSpan.FromMinutes(1),
            ex =>
            {
                _log.Warning("QueryActor pao ({0}). Restartujem ga.", ex.Message);
                return Directive.Restart;
            });

    private void Handle(Messages.RegisterSearchQuery msg)
    {
        var query = msg.Query.Trim();
        if (!_queries.ContainsKey(query))
        {
            // Nov pojam => napravi aktora i posmatraj ga (Death Watch)
            var child = Context.ActorOf(QueryActor.Props(query, _model), ChildName(query));
            Context.Watch(child);
            _queries[query] = child;
            _log.Info("Registrovan novi pojam '{0}'. Trenutno se prati {1} pojmova.", query, _queries.Count);
        }

        Sender.Tell(new Messages.SearchRegistered(query));
    }

    // Death Watch: kad se posmatrani aktor zaustavi, ukloni ga iz mape
    private void Handle(Terminated t)
    {
        var entry = _queries.FirstOrDefault(kv => kv.Value.Equals(t.ActorRef));
        if (entry.Key is not null)
        {
            _queries.Remove(entry.Key);
            _log.Warning("QueryActor za '{0}' je zaustavljen i uklonjen.", entry.Key);
        }
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
