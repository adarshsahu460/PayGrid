import axios from 'axios';

const TOKENIZATION_URL = process.env.TOKENIZATION_URL || 'http://tokenization-service:3007/detokenize';

export async function detokenize(cardToken: string): Promise<string | null> {
  try {
    const resp = await axios.post(TOKENIZATION_URL, { token: cardToken });
    return resp.data.cardNumber;
  } catch (err) {
    return null;
  }
}
