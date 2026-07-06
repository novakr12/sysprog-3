using System.Net.Http.Json;

namespace BookSentiment.Services;

// Knjiga iz Google odgovora
public sealed record GoogleBookVolume(string Id, string Title, string? Description);

// Klijent za Google Books API
public sealed class GoogleBooksClient
{
    private const string BaseUrl = "https://www.googleapis.com/books/v1/volumes";
    private readonly HttpClient _http;
    private readonly string? _apiKey;

    public GoogleBooksClient(HttpClient http, string? apiKey = null)
    {
        _http = http;
        _apiKey = string.IsNullOrWhiteSpace(apiKey) ? null : apiKey;
    }

    // Poziva API i vraca knjige
    public async Task<IReadOnlyList<GoogleBookVolume>> SearchAsync(
        string query, int maxResults = 20, CancellationToken ct = default)
    {
        // Sastavi URL (kljuc samo ako postoji)
        var url = $"{BaseUrl}?q={Uri.EscapeDataString(query)}&maxResults={maxResults}&country=US";
        if (_apiKey is not null)
            url += $"&key={_apiKey}";

        using var response = await _http.GetAsync(url, ct).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();   // greska ako nije 2xx

        // Parsiraj JSON
        var payload = await response.Content
            .ReadFromJsonAsync<GoogleBooksResponse>(cancellationToken: ct)
            .ConfigureAwait(false);

        if (payload?.Items is null)
            return Array.Empty<GoogleBookVolume>();

        // Prevedi u nas tip
        var volumes = new List<GoogleBookVolume>(payload.Items.Count);
        foreach (var item in payload.Items)
        {
            var info = item.VolumeInfo;
            if (info is null)
                continue;

            volumes.Add(new GoogleBookVolume(
                Id: item.Id ?? Guid.NewGuid().ToString("N"),
                Title: info.Title ?? "(untitled)",
                Description: info.Description));
        }

        return volumes;
    }

    // Delovi Google JSON-a
    private sealed record GoogleBooksResponse(List<VolumeItem>? Items);

    private sealed record VolumeItem(string? Id, VolumeInfo? VolumeInfo);

    private sealed record VolumeInfo(string? Title, string? Description);
}
