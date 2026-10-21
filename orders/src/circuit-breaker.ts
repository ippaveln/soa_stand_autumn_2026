export class CircuitBreaker {
  private failures = 0;
  private openUntil = 0;
  private probeRunning = false;

  allow(now = Date.now()): boolean {
    if (this.openUntil === 0) return true;
    if (now < this.openUntil || this.probeRunning) return false;
    this.probeRunning = true;
    return true;
  }

  success(): void {
    this.failures = 0;
    this.openUntil = 0;
    this.probeRunning = false;
  }

  failure(now = Date.now()): void {
    this.failures++;
    if (this.probeRunning || this.failures >= 3) this.openUntil = now + 5000;
    this.probeRunning = false;
  }
}
