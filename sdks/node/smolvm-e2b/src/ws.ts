/**
 * Minimal, dependency-free WebSocket client (RFC 6455) over a raw duplex socket —
 * enough to drive the sandbox interactive PTY endpoint over either a Unix socket
 * or TCP. Handles unfragmented text/binary/close/ping frames, which is all the
 * server sends; it does not implement fragmentation or extensions.
 */
import { randomBytes } from "node:crypto";
import type { Duplex } from "node:stream";

export interface WsHandlers {
  onBinary?: (data: Buffer) => void;
  onText?: (data: string) => void;
  onClose?: (code?: number) => void;
  onError?: (err: Error) => void;
}

const OP_TEXT = 0x1;
const OP_BINARY = 0x2;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xa;

export class WsConn {
  private handlers: WsHandlers = {};
  private rx: Buffer;
  private closed = false;

  constructor(
    private sock: Duplex,
    initial: Buffer,
  ) {
    this.rx = initial.length ? Buffer.from(initial) : Buffer.alloc(0);
    this.sock.on("data", (d: Buffer) => {
      this.rx = Buffer.concat([this.rx, d]);
      this.drain();
    });
    this.sock.on("close", () => this.emitClose());
    this.sock.on("error", (e: Error) => this.handlers.onError?.(e));
    this.drain();
  }

  on(h: WsHandlers): void {
    this.handlers = { ...this.handlers, ...h };
  }

  sendBinary(data: Buffer): void {
    this.sendFrame(OP_BINARY, data);
  }

  sendText(s: string): void {
    this.sendFrame(OP_TEXT, Buffer.from(s, "utf8"));
  }

  /** Send a ping frame (keepalive). The peer is expected to pong. */
  ping(payload: Buffer = Buffer.alloc(0)): void {
    this.sendFrame(OP_PING, payload);
  }

  close(code = 1000): void {
    if (this.closed) return;
    const b = Buffer.alloc(2);
    b.writeUInt16BE(code, 0);
    this.sendFrame(OP_CLOSE, b);
    this.closed = true;
    this.sock.end();
  }

  private sendFrame(opcode: number, payload: Buffer): void {
    if (this.closed && opcode !== OP_CLOSE) return;
    const len = payload.length;
    let header: Buffer;
    if (len < 126) {
      header = Buffer.alloc(2);
      header[1] = 0x80 | len;
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[1] = 0x80 | 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[1] = 0x80 | 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    header[0] = 0x80 | opcode; // FIN + opcode
    const mask = randomBytes(4);
    const masked = Buffer.allocUnsafe(len);
    for (let i = 0; i < len; i++) masked[i] = payload[i] ^ mask[i & 3];
    this.sock.write(Buffer.concat([header, mask, masked]));
  }

  private drain(): void {
    for (;;) {
      if (this.rx.length < 2) return;
      const b1 = this.rx[1];
      const opcode = this.rx[0] & 0x0f;
      const isMasked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;
      if (len === 126) {
        if (this.rx.length < 4) return;
        len = this.rx.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (this.rx.length < 10) return;
        len = Number(this.rx.readBigUInt64BE(2));
        offset = 10;
      }
      let maskKey: Buffer | undefined;
      if (isMasked) {
        if (this.rx.length < offset + 4) return;
        maskKey = this.rx.subarray(offset, offset + 4);
        offset += 4;
      }
      if (this.rx.length < offset + len) return;
      let payload = Buffer.from(this.rx.subarray(offset, offset + len));
      if (maskKey) {
        for (let i = 0; i < len; i++) payload[i] ^= maskKey[i & 3];
      }
      this.rx = this.rx.subarray(offset + len);
      this.handleFrame(opcode, payload);
    }
  }

  private handleFrame(opcode: number, payload: Buffer): void {
    switch (opcode) {
      case OP_TEXT:
        this.handlers.onText?.(payload.toString("utf8"));
        break;
      case OP_BINARY:
        this.handlers.onBinary?.(payload);
        break;
      case OP_CLOSE: {
        const code = payload.length >= 2 ? payload.readUInt16BE(0) : undefined;
        this.closed = true;
        this.emitClose(code);
        this.sock.end();
        break;
      }
      case OP_PING:
        this.sendFrame(OP_PONG, payload);
        break;
      case OP_PONG:
        break;
    }
  }

  private emitClose(code?: number): void {
    const h = this.handlers.onClose;
    if (h) {
      this.handlers.onClose = undefined;
      h(code);
    }
  }
}
