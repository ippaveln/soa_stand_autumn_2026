export async function call(
  url: string,
  method = "GET",
  body?: unknown,
  key?: string,
  timeoutMs = 4000,
): Promise<{ status: number; body: unknown }> {
  const response = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json",
      ...(key ? { "Idempotency-Key": key } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : null };
}

export function retryDelay(attempts: number): number {
  return Math.min(500 * 2 ** Math.min(attempts, 4), 5000);
}
