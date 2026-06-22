// Body builders for every payment method, matching the wire format
// internal/adapter/http/dto.go expects. Each call produces a body with a
// fresh eventId/Idempotency-Key — without that, dedup would collapse every
// iteration into a single outbox row and the load test would measure
// nothing.

export const METHODS = ["PIX", "BOLETO", "TRANSFER", "CARTAO_CREDITO", "CARTAO_DEBITO"];

let counter = 0;

function uniqueSuffix() {
  counter += 1;
  return `${Date.now()}-${__VU}-${__ITER}-${counter}`;
}

function envelope(method, suffix, extra) {
  const body = Object.assign(
    {
      eventId: `evt-${suffix}`,
      provider: { name: "LOADTEST", providerPaymentId: `prov-${suffix}` },
      payment: {
        paymentId: `pay-${suffix}`,
        amount: 100.5,
        currency: "BRL",
        method,
      },
      occurredAt: new Date().toISOString(),
    },
    extra
  );
  body.__idempotencyKey = `loadtest-${suffix}`;
  return body;
}

function pix(suffix) {
  return envelope("PIX", suffix, {
    pix: { endToEndId: "E00000000000000000000000000", txid: `ORDER-${suffix}` },
  });
}

function boleto(suffix) {
  return envelope("BOLETO", suffix, {
    boleto: {
      barcode: "00000000000000000000000000000000000000000000",
      dueDate: "2026-12-31",
      payerDocument: "00000000000",
    },
  });
}

function transfer(suffix) {
  const body = envelope("TRANSFER", suffix, {});
  body.payment.payerId = "018f7f9e-6e8b-7c3a-8f2a-000000000001";
  body.payment.recipientId = "018f7f9e-6e8b-7c3a-8f2a-000000000002";
  return body;
}

function cartaoCredito(suffix) {
  return envelope("CARTAO_CREDITO", suffix, {
    cartao_credito: { cardNumber: "4111111111111111", cardType: "CREDIT", cardIssuer: "VISA" },
  });
}

function cartaoDebito(suffix) {
  return envelope("CARTAO_DEBITO", suffix, {
    cartao_debito: { cardNumber: "4111111111111111", cardType: "DEBIT", cardIssuer: "MASTERCARD" },
  });
}

const builders = {
  PIX: pix,
  BOLETO: boleto,
  TRANSFER: transfer,
  CARTAO_CREDITO: cartaoCredito,
  CARTAO_DEBITO: cartaoDebito,
};

// buildBody returns a fresh, valid body for method, tagged with a unique
// eventId/Idempotency-Key (body.__idempotencyKey — strip before sending if
// the target doesn't tolerate unknown fields; the ingestion-api DTO ignores
// unrecognized top-level keys so this is safe to send as-is).
export function buildBody(method) {
  const builder = builders[method];
  if (!builder) {
    throw new Error(`unknown method ${method}`);
  }
  return builder(uniqueSuffix());
}
