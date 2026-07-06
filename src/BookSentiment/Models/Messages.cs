namespace BookSentiment.Models;

// Poruke koje komponente salju jedna drugoj
public static class Messages
{
    // Pocni pracenje pojma
    public sealed record RegisterSearchQuery(string Query);

    // Potvrda registracije
    public sealed record SearchRegistered(string Query);

    // Trazi rezultate za pojam
    public sealed record GetResults(string Query);

    // Trazi listu pojmova
    public sealed record GetActiveQueries;

    // Lista pojmova
    public sealed record ActiveQueries(IReadOnlyList<string> Queries);

    // Jedan opis knjige
    public sealed record BookDescriptionMessage(
        string Query,
        string BookId,
        string Title,
        string Description);

    // Izracunaj sentiment
    public sealed record AnalyzeSentiment(string Query, string BookId, string Text);

    // Rezultat sentimenta
    public sealed record SentimentComputed(
        string Query,
        string BookId,
        string Label,
        float Confidence,
        float Score);

    // Stanje za pojam
    public sealed record QueryResults(
        string Query,
        int TotalBooks,
        int Positive,
        int Negative,
        double AverageConfidence,
        IReadOnlyList<BookResult> Books,
        DateTimeOffset LastUpdatedUtc);

    // Pojam se ne prati
    public sealed record QueryNotFound(string Query);
}
