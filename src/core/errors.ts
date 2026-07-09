/** All errors thrown by Ripply are instances of RipplyError. */
export class RipplyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RipplyError';
  }
}
