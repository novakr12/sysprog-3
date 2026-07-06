using BookSentiment.Models;
using Microsoft.ML;

namespace BookSentiment.Services;

// ML model za sentiment (trenira se jednom, deli se svima)
public sealed class SentimentModel
{
    private readonly MLContext _mlContext;
    private readonly ITransformer _model;
    private readonly DataViewSchema _schema;

    private SentimentModel(MLContext mlContext, ITransformer model, DataViewSchema schema)
    {
        _mlContext = mlContext;
        _model = model;
        _schema = schema;
    }

    // Trenira model iz TSV fajla
    public static SentimentModel Train(string dataPath)
    {
        if (!File.Exists(dataPath))
            throw new FileNotFoundException($"Sentiment training data not found at '{dataPath}'.", dataPath);

        var mlContext = new MLContext(seed: 1);

        // Ucitaj podatke
        IDataView data = mlContext.Data.LoadFromTextFile<SentimentInput>(
            dataPath,
            hasHeader: true,
            separatorChar: '\t');

        // Tekst -> brojevi, pa klasifikator
        var pipeline = mlContext.Transforms.Text
            .FeaturizeText("Features", nameof(SentimentInput.Text))
            .Append(mlContext.BinaryClassification.Trainers.SdcaLogisticRegression(
                labelColumnName: "Label",
                featureColumnName: "Features"));

        ITransformer model = pipeline.Fit(data);
        return new SentimentModel(mlContext, model, data.Schema);
    }

    // Svaki aktor pravi svoj engine (nije thread-safe)
    public PredictionEngine<SentimentInput, SentimentPrediction> CreateEngine() =>
        _mlContext.Model.CreatePredictionEngine<SentimentInput, SentimentPrediction>(_model, _schema);
}
