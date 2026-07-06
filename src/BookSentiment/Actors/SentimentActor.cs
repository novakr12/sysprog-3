using Akka.Actor;
using Akka.Event;
using BookSentiment.Models;
using BookSentiment.Services;
using Microsoft.ML;

namespace BookSentiment.Actors;

// Racuna sentiment za jedan opis
public sealed class SentimentActor : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly PredictionEngine<SentimentInput, SentimentPrediction> _engine;

    public SentimentActor(SentimentModel model)
    {
        _engine = model.CreateEngine();

        Receive<Messages.AnalyzeSentiment>(Handle);
    }

    private void Handle(Messages.AnalyzeSentiment msg)
    {
        var prediction = _engine.Predict(new SentimentInput { Text = msg.Text });

        // Labela i pouzdanost
        var label = prediction.Prediction ? "Positive" : "Negative";
        var confidence = prediction.Prediction ? prediction.Probability : 1f - prediction.Probability;

        _log.Debug("Sentiment za knjigu {0} ('{1}'): {2} ({3:P1})",
            msg.BookId, msg.Query, label, confidence);

        // Posalji rezultat nazad
        Sender.Tell(new Messages.SentimentComputed(
            msg.Query, msg.BookId, label, confidence, prediction.Score));
    }

    public static Props Props(SentimentModel model) =>
        Akka.Actor.Props.Create(() => new SentimentActor(model));
}
