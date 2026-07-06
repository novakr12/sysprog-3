using Microsoft.ML.Data;

namespace BookSentiment.Models;

// Analizirana knjiga
public sealed record BookResult(
    string BookId,
    string Title,
    string Description,
    string Sentiment,
    float Confidence,
    float Score,
    DateTimeOffset AnalysedUtc);

// ML.NET ulaz
public sealed class SentimentInput
{
    [LoadColumn(0)]                       // tekst
    public string Text { get; set; } = string.Empty;

    [LoadColumn(1)]                       // 0/1
    [ColumnName("Label")]
    public bool Sentiment { get; set; }
}

// ML.NET izlaz
public sealed class SentimentPrediction
{
    [ColumnName("PredictedLabel")]
    public bool Prediction { get; set; }  // true = pozitivan

    public float Probability { get; set; } // verovatnoca (0-1)

    public float Score { get; set; }
}
