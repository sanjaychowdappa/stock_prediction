const express = require("express");
const http = require("http");
const axios = require("axios");
const WS = require("ws");
const path = require("path");
const cors = require("cors");
const fs = require("fs");

function loadEnvFile() {
  const envPath = path.join(__dirname, ".env");
  if (!fs.existsSync(envPath)) return;
  const lines = fs.readFileSync(envPath, "utf8").split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq === -1) continue;
    const key = trimmed.slice(0, eq).trim();
    const value = trimmed.slice(eq + 1).trim().replace(/^["']|["']$/g, "");
    if (key && process.env[key] == null) process.env[key] = value;
  }
}

loadEnvFile();

const app = express();
app.use(cors());

const MARKET_UNIVERSE = [
  { symbol: "AAPL", name: "Apple", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "MSFT", name: "Microsoft", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "NVDA", name: "NVIDIA", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "AMZN", name: "Amazon", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "META", name: "Meta Platforms", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "GOOG", name: "Alphabet Class C", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "GOOGL", name: "Alphabet Class A", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "AVGO", name: "Broadcom", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "TSLA", name: "Tesla", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "COST", name: "Costco", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "NFLX", name: "Netflix", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "AMD", name: "Advanced Micro Devices", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "ADBE", name: "Adobe", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "PEP", name: "PepsiCo", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "CSCO", name: "Cisco", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "INTC", name: "Intel", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "QCOM", name: "Qualcomm", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "AMAT", name: "Applied Materials", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "TXN", name: "Texas Instruments", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "BKNG", name: "Booking Holdings", exchange: "NASDAQ", indexes: ["NASDAQ-100", "S&P 500"] },
  { symbol: "JPM", name: "JPMorgan Chase", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "V", name: "Visa", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "MA", name: "Mastercard", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "UNH", name: "UnitedHealth Group", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "LLY", name: "Eli Lilly", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "XOM", name: "Exxon Mobil", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "WMT", name: "Walmart", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "JNJ", name: "Johnson & Johnson", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "PG", name: "Procter & Gamble", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "HD", name: "Home Depot", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "BAC", name: "Bank of America", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "KO", name: "Coca-Cola", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "MRK", name: "Merck", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "CVX", name: "Chevron", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "ABBV", name: "AbbVie", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "CRM", name: "Salesforce", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "ORCL", name: "Oracle", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "MCD", name: "McDonald's", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "DIS", name: "Disney", exchange: "NYSE", indexes: ["S&P 500"] },
  { symbol: "NKE", name: "Nike", exchange: "NYSE", indexes: ["S&P 500"] },
];
const AVAILABLE_SYMBOLS = MARKET_UNIVERSE.map((item) => item.symbol);
const SYMBOLS = new Set(AVAILABLE_SYMBOLS);
/** Finnhub uses different tickers for some names; we keep UI symbols stable. */
const FINNHUB_SUBSCRIBE = { GOOG: "GOOGL" };
const FINNHUB_FROM_VENDOR = { GOOGL: "GOOG" };

const API_KEY = (process.env.ALPHA_VANTAGE_KEY || "").trim() || "demo";
const USING_DEMO_KEY = API_KEY === "demo";
const FINNHUB_KEY = (process.env.FINNHUB_API_KEY || "").trim();
const ALLOW_SIMULATED = process.env.ALLOW_SIMULATED === "true";

/** Latest trade per UI symbol: { price, t } (t = wall-clock ms of trade) */
const latestTradeBySymbol = new Map();
/** Ignore trades older than this when treating Finnhub as "live" */
const FINNHUB_STALE_MS = 120000;

let lastRateLimitLog = 0;
function logApiHint(symbol, detail) {
  const now = Date.now();
  if (now - lastRateLimitLog < 120000) return;
  lastRateLimitLog = now;
  if (USING_DEMO_KEY) {
    console.warn(
      `[stock-predictor] No quote for ${symbol} (${detail}). Using simulated data. Set ALPHA_VANTAGE_KEY (real key) for live quotes — see .env.example`,
    );
  } else {
    console.warn(
      `[stock-predictor] No quote for ${symbol} (${detail}). Using simulated data — check key, limits, or market hours.`,
    );
  }
}

const SIM_BASE = {
  AAPL: 178.5,
  GOOG: 165.2,
  GOOGL: 164.8,
  MSFT: 415.0,
  NVDA: 875.0,
  AMZN: 185.0,
  TSLA: 248.0,
  META: 485.0,
  AVGO: 1320.0,
  COST: 735.0,
  NFLX: 620.0,
  AMD: 155.0,
  ADBE: 500.0,
  PEP: 175.0,
  CSCO: 50.0,
  INTC: 36.0,
  QCOM: 170.0,
  AMAT: 205.0,
  TXN: 180.0,
  BKNG: 3600.0,
  JPM: 198.0,
  V: 280.0,
  MA: 455.0,
  UNH: 510.0,
  LLY: 770.0,
  XOM: 116.0,
  WMT: 60.0,
  JNJ: 152.0,
  PG: 160.0,
  HD: 345.0,
  BAC: 38.0,
  KO: 62.0,
  MRK: 128.0,
  CVX: 160.0,
  ABBV: 170.0,
  CRM: 285.0,
  ORCL: 125.0,
  MCD: 285.0,
  DIS: 112.0,
  NKE: 95.0,
};

app.use(express.static(path.join(__dirname, "client_build")));

app.get("/api/symbols", (req, res) => {
  res.json(MARKET_UNIVERSE);
});

app.get("/api/market", (req, res) => {
  const nasdaq = MARKET_UNIVERSE.filter((item) =>
    item.indexes.includes("NASDAQ-100"),
  ).length;
  const sp500 = MARKET_UNIVERSE.filter((item) =>
    item.indexes.includes("S&P 500"),
  ).length;
  res.json({
    symbols: MARKET_UNIVERSE,
    counts: { nasdaq, sp500, total: MARKET_UNIVERSE.length },
    dataSources: {
      liveTrades: Boolean(FINNHUB_KEY),
      yahooChart: true,
      intradayBars: !USING_DEMO_KEY,
      alphaVantage: USING_DEMO_KEY ? "demo" : "configured",
    },
  });
});

app.get("/api/client-config", (req, res) => {
  res.json({
    alphaVantageKey: USING_DEMO_KEY ? "" : API_KEY,
    hasFinnhubKey: Boolean(FINNHUB_KEY),
  });
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "client_build", "index.html"));
});

const server = http.createServer(app);
const wss = new WS.Server({ server });
const clientSubscriptions = {};

function connectFinnhubStream() {
  if (!FINNHUB_KEY) {
    console.warn(
      "[stock-predictor] FINNHUB_API_KEY not set — no live trade stream. Add a free key from https://finnhub.io/register (US stocks stream in real time when markets are open).",
    );
    return;
  }

  const url = `wss://ws.finnhub.io?token=${encodeURIComponent(FINNHUB_KEY)}`;
  const fh = new WS(url);

  fh.on("open", () => {
    console.log("[stock-predictor] Finnhub trade stream connected");
    for (const sym of AVAILABLE_SYMBOLS) {
      const sub = FINNHUB_SUBSCRIBE[sym] || sym;
      fh.send(JSON.stringify({ type: "subscribe", symbol: sub }));
    }
  });

  fh.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "ping") {
        fh.send(JSON.stringify({ type: "pong" }));
        return;
      }
      if (msg.type === "trade" && Array.isArray(msg.data)) {
        for (const row of msg.data) {
          const vendorSym = row.s;
          const p = row.p;
          const t = row.t;
          if (vendorSym == null || typeof p !== "number" || typeof t !== "number") continue;
          const sym = FINNHUB_FROM_VENDOR[vendorSym] || vendorSym;
          if (!AVAILABLE_SYMBOLS.includes(sym)) continue;
          latestTradeBySymbol.set(sym, { price: p, t });
        }
      }
    } catch {
      /* ignore */
    }
  });

  fh.on("close", () => {
    console.warn("[stock-predictor] Finnhub stream closed; reconnecting in 5s…");
    setTimeout(connectFinnhubStream, 5000);
  });

  fh.on("error", (err) => {
    console.warn("[stock-predictor] Finnhub stream error:", err.message);
  });
}

const simState = {};
function nextSimulatedPrice(symbol) {
  const base = SIM_BASE[symbol] ?? 100;
  if (simState[symbol] == null) simState[symbol] = base;
  const sigma = base * 0.0015;
  simState[symbol] += (Math.random() - 0.5) * sigma * 2;
  simState[symbol] = Math.max(1, Math.round(simState[symbol] * 100) / 100);
  const time = new Date().toISOString().replace("T", " ").slice(0, 19);
  return { time, price: simState[symbol], source: "simulated" };
}

const API_MIN_INTERVAL_MS = 60000;
const lastApiFetch = {};

function getFinnhubPricePoint(symbol) {
  if (!FINNHUB_KEY) return null;
  const row = latestTradeBySymbol.get(symbol);
  if (!row) return null;
  if (Date.now() - row.t > FINNHUB_STALE_MS) return null;
  const time = new Date(row.t).toISOString().replace("T", " ").slice(0, 23);
  return {
    time,
    price: row.price,
    source: "finnhub",
    feed: "live_trade",
  };
}

async function tryAlphaVantage(symbol) {
  try {
    const intraUrl =
      `https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY` +
      `&symbol=${encodeURIComponent(symbol)}&interval=1min&outputsize=compact&apikey=${API_KEY}`;
    const { data } = await axios.get(intraUrl, { timeout: 10000 });
    if (data.Note || data.Information) {
      logApiHint(symbol, data.Note || data.Information);
    } else {
      const series =
        data &&
        (data["Time Series (1min)"] ||
          data["Time Series (5min)"] ||
          data["Time Series (Daily)"]);
      if (series) {
        const bars = Object.keys(series)
          .sort()
          .map((time) => {
            const row = series[time];
            const price = parseFloat(row["4. close"] || row["1. open"]);
            const volume = parseInt(row["5. volume"] || "0", 10);
            return { time, price, volume };
          })
          .filter((bar) => Number.isFinite(bar.price))
          .slice(-30);
        const latest = bars[bars.length - 1];
        if (latest) {
          return {
            time: latest.time,
            price: latest.price,
            source: "alphavantage",
            feed: "intraday",
            bars,
          };
        }
      }
    }
  } catch {
    /* fall through */
  }

  try {
    const quoteUrl = `https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=${encodeURIComponent(symbol)}&apikey=${API_KEY}`;
    const { data } = await axios.get(quoteUrl, { timeout: 10000 });
    if (data.Note || data.Information) {
      logApiHint(symbol, data.Note || data.Information);
      return null;
    }
    const g = data["Global Quote"];
    if (g && g["05. price"]) {
      const price = parseFloat(g["05. price"]);
      if (!Number.isFinite(price)) return null;
      const day = g["07. latest trading day"] || "";
      return {
        time: day ? `${day} (prior session / last sale)` : "",
        price,
        source: "alphavantage",
        feed: "eod",
        bars: [],
      };
    }
  } catch {
    return null;
  }
  return null;
}

async function tryYahooFinance(symbol) {
  try {
    const url =
      `https://query1.finance.yahoo.com/v8/finance/chart/` +
      `${encodeURIComponent(symbol)}?interval=1m&range=1d&includePrePost=true`;
    const { data } = await axios.get(url, {
      timeout: 10000,
      headers: { "User-Agent": "Mozilla/5.0" },
    });
    const result = data && data.chart && data.chart.result && data.chart.result[0];
    if (!result) return null;

    const timestamps = result.timestamp || [];
    const quote =
      result.indicators &&
      result.indicators.quote &&
      result.indicators.quote[0];
    const closes = quote && quote.close ? quote.close : [];
    const volumes = quote && quote.volume ? quote.volume : [];
    const bars = timestamps
      .map((stamp, index) => ({
        time: new Date(stamp * 1000).toISOString().replace("T", " ").slice(0, 19),
        price: closes[index],
        volume: volumes[index] || 0,
      }))
      .filter((bar) => Number.isFinite(bar.price))
      .slice(-30);
    const latest = bars[bars.length - 1];
    const metaPrice =
      result.meta && Number.isFinite(result.meta.regularMarketPrice)
        ? result.meta.regularMarketPrice
        : null;
    const metaTime =
      result.meta && result.meta.regularMarketTime
        ? new Date(result.meta.regularMarketTime * 1000)
            .toISOString()
            .replace("T", " ")
            .slice(0, 19)
        : null;

    if (!latest && metaPrice == null) return null;
    return {
      time: latest ? latest.time : metaTime,
      price: latest ? latest.price : metaPrice,
      source: "yahoo",
      feed: "intraday",
      bars,
    };
  } catch (err) {
    console.warn(`[stock-predictor] Yahoo chart unavailable for ${symbol}: ${err.message}`);
    return null;
  }
}

function linearPrediction(prices, stepsAhead) {
  if (prices.length < 2) return prices[prices.length - 1] || 0;
  const n = prices.length;
  const avgX = (n - 1) / 2;
  const avgY = prices.reduce((sum, value) => sum + value, 0) / n;
  let numerator = 0;
  let denominator = 0;
  for (let i = 0; i < n; i += 1) {
    numerator += (i - avgX) * (prices[i] - avgY);
    denominator += (i - avgX) ** 2;
  }
  const slope = denominator ? numerator / denominator : 0;
  return prices[n - 1] + slope * stepsAhead;
}

function percentChange(from, to) {
  if (!from || !to) return 0;
  return ((to - from) / from) * 100;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance =
    values.reduce((sum, value) => sum + (value - avg) ** 2, 0) /
    (values.length - 1);
  return Math.sqrt(variance);
}

function buildSignal(points, latestPoint) {
  const usable = points.length
    ? points
    : [{ time: latestPoint.time, price: latestPoint.price, volume: 0 }];
  const recent = usable.slice(-8);
  const prices = recent.map((point) => point.price);
  const current = latestPoint.price;
  const fiveMinuteBase = prices.length >= 6 ? prices[prices.length - 6] : prices[0];
  const predicted5m = linearPrediction(prices, Math.max(1, 6 - Math.min(prices.length, 6)));
  const momentumPct = percentChange(fiveMinuteBase, current);
  const predictedChangePct = percentChange(current, predicted5m);
  const returns = [];
  for (let i = 1; i < prices.length; i += 1) {
    returns.push(percentChange(prices[i - 1], prices[i]));
  }
  const volatilityPct = standardDeviation(returns);
  const threshold = Math.max(0.08, volatilityPct * 0.85);
  let action = "HOLD";
  if (predictedChangePct > threshold && momentumPct > -threshold / 2) {
    action = "WATCH_BUY";
  } else if (predictedChangePct < -threshold && momentumPct < threshold / 2) {
    action = "WATCH_SELL";
  }

  const strength = Math.min(1, Math.abs(predictedChangePct) / (threshold * 2 || 1));
  const dataScore = Math.min(1, usable.length / 8);
  const confidence = Math.round((45 + strength * 35 + dataScore * 20) * 10) / 10;
  const stopLossPct =
    action === "WATCH_BUY" ? -Math.max(0.15, volatilityPct * 1.2) : null;
  const takeProfitPct =
    action === "WATCH_BUY" ? Math.max(0.2, volatilityPct * 1.6) : null;

  return {
    timeframe: "next_5_min",
    action,
    current,
    predicted5m: Math.round(predicted5m * 100) / 100,
    predictedChangePct: Math.round(predictedChangePct * 1000) / 1000,
    momentumPct: Math.round(momentumPct * 1000) / 1000,
    volatilityPct: Math.round(volatilityPct * 1000) / 1000,
    thresholdPct: Math.round(threshold * 1000) / 1000,
    confidence,
    risk:
      volatilityPct > 0.45
        ? "high"
        : volatilityPct > 0.2
          ? "medium"
          : "low",
    stopLossPct,
    takeProfitPct,
    explanation:
      action === "WATCH_BUY"
        ? "Short-term trend is pointing up enough to clear the volatility filter."
        : action === "WATCH_SELL"
          ? "Short-term trend is pointing down enough to clear the volatility filter."
          : "Forecast edge is too small versus recent noise, so the safer signal is hold/watch.",
  };
}

function unavailableSignal() {
  return {
    timeframe: "next_5_min",
    action: "HOLD",
    current: null,
    predicted5m: null,
    predictedChangePct: null,
    momentumPct: null,
    volatilityPct: null,
    thresholdPct: null,
    confidence: null,
    risk: "unavailable",
    stopLossPct: null,
    takeProfitPct: null,
    explanation:
      "Real market data is unavailable right now, so no buy or sell signal is being calculated.",
  };
}

function simulatedSeries(symbol) {
  const points = [];
  for (let i = 29; i >= 0; i -= 1) {
    const point = nextSimulatedPrice(symbol);
    const time = new Date(Date.now() - i * 60000)
      .toISOString()
      .replace("T", " ")
      .slice(0, 19);
    points.push({ ...point, time });
  }
  return points;
}

async function getPricePoint(symbol) {
  const fromStream = getFinnhubPricePoint(symbol);

  const now = Date.now();
  const due =
    !lastApiFetch[symbol] || now - lastApiFetch[symbol].at >= API_MIN_INTERVAL_MS;

  if (due) {
    const live = (await tryYahooFinance(symbol)) || (await tryAlphaVantage(symbol));
    lastApiFetch[symbol] = { at: now, live };
  }

  const cached = lastApiFetch[symbol];
  let point = null;
  let bars = [];

  if (cached && cached.live) {
    point = cached.live;
    bars = cached.live.bars || [];
  }

  if (fromStream) {
    point = fromStream;
    if (bars.length) {
      bars = [...bars.slice(-29), { time: fromStream.time, price: fromStream.price }];
    }
  }

  if (!point && ALLOW_SIMULATED) {
    bars = simulatedSeries(symbol);
    point = bars[bars.length - 1];
  }

  if (!point) {
    return {
      time: "",
      price: null,
      source: "unavailable",
      feed: null,
      bars: [],
      signal: unavailableSignal(),
    };
  }

  return {
    ...point,
    bars,
    signal: buildSignal(bars, point),
  };
}

wss.on("connection", (ws) => {
  let currentSymbol = AVAILABLE_SYMBOLS[0];
  clientSubscriptions[ws] = currentSymbol;

  ws.on("message", (msg) => {
    try {
      const data = JSON.parse(msg);
      if (data.type === "subscribe" && SYMBOLS.has(data.symbol)) {
        currentSymbol = data.symbol;
        clientSubscriptions[ws] = data.symbol;
      }
    } catch {
      /* ignore */
    }
  });

  ws.on("close", () => {
    delete clientSubscriptions[ws];
  });
});

async function broadcastPrices() {
  const clients = [...wss.clients].filter((c) => c.readyState === WS.OPEN);
  const symbolsNeeded = [
    ...new Set(
      clients.map((ws) => clientSubscriptions[ws] || AVAILABLE_SYMBOLS[0]),
    ),
  ];

  const bySymbol = {};
  for (const sym of symbolsNeeded) {
    bySymbol[sym] = await getPricePoint(sym);
  }

  for (const ws of clients) {
    const symbol = clientSubscriptions[ws] || AVAILABLE_SYMBOLS[0];
    const received = bySymbol[symbol];
    if (!received) continue;

    ws.send(
      JSON.stringify({
        time: received.time,
        price: received.price,
        predicted: received.signal.predicted5m,
        symbol,
        source: received.source,
        feed: received.feed || null,
        signal: received.signal,
        history: (received.bars || []).map((bar) => ({
          time: bar.time,
          price: bar.price,
          volume: bar.volume || 0,
        })),
      }),
    );
  }

  setTimeout(broadcastPrices, 5000);
}
broadcastPrices();

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log("Server listening on port", PORT);
  if (USING_DEMO_KEY) {
    console.warn(
      "[stock-predictor] ALPHA_VANTAGE_KEY is not set — using built-in demo key (heavy rate limits). Copy .env.example to .env and add your key.",
    );
  } else {
    console.log("[stock-predictor] ALPHA_VANTAGE_KEY is set; used as fallback when Finnhub has no fresh trades.");
  }
  connectFinnhubStream();
});
