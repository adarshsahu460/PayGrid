import express from 'express';
import dotenv from 'dotenv';
dotenv.config();

const app = express();
app.use(express.json());

const ipHistory: Map<string, number[]> = new Map();

app.post('/analyze', (req, res) => {
  const ip = req.ip || 'unknown'; // Provide a default value for ip
  const now = Date.now();
  const history = ipHistory.get(ip) || [];
  // Remove entries older than 1 minute
  const recent = history.filter(ts => now - ts < 60000);
  recent.push(now);
  ipHistory.set(ip, recent);
  if (recent.length > 3) {
    return res.json({ risk: 'high' });
  }
  return res.json({ risk: 'low' });
});

const PORT = process.env.PORT || 3008;
app.listen(PORT, () => {
  console.log(`Fraud Detection Service listening on port ${PORT}`);
});
