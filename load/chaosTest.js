import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';

export const options = {
  vus: 400,
  iterations: 400,
};
//redis-cli --scan | while read k; do v=$(redis-cli GET "$k"); if [ "$v" = "PAID" ]; then echo "$k"; fi; done


export function setup() {
  const users = http.get('http://localhost:8080/users').json();
  const phones = http.get('http://localhost:8080/phones').json();
  //shorthand for { users: users, phones: phones }
  return { users, phones };
}

const successfulPayment = new Counter('Paid');
const failedPayment = new Counter('Failed');
const soldOut = new Counter('SoldOut');
const badUserInput = new Counter('BadUserInput');

export default function (data) {
  const user = data.users[Math.floor(Math.random() * data.users.length)];
  const phone = data.phones[0];

  // Random bad inputs on buy-request
  let payload;
  const chaosBuy = Math.random();

  if (chaosBuy < 0.1) {
    // Broken JSON (invalid JSON string)
    payload = `{broken json`; // intentionally truncated

  } else if (chaosBuy < 0.2) {
    // Wrong type (phoneUUID should be string, sending number instead)
    payload = JSON.stringify({
      userUUID: user.userUUID,
      phoneUUID: 99999,
    });

  } else {
    // Normal
    payload = JSON.stringify({
      userUUID: user.userUUID,
      phoneUUID: phone.phoneUUID,
    });
  }

  const createRes = http.post('http://localhost:8080/buy-request', payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  if (Math.random() < 0.1) {
    console.log(`Chaos: Double calling buy-request`);
    http.post('http://localhost:8080/buy-request', payload, {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  if (createRes.status !== 200) {
    badUserInput.add(1);
    return;
  }

  const ticketUUID = createRes.json('ticketUUID');
  let webhookCalled = false;

  // Polling for status
while (true) {
  const statusRes = http.get(`http://localhost:8080/status/${ticketUUID}`);


  const status = statusRes.json('status');

  if (status == "SUCCESSFUL_RESERVATION") {
    if (!webhookCalled) {
      const success = Math.random() < 0.8;
      const chaosWebhook = Math.random();

      if (chaosWebhook < 0.1) {
        callStripeWebhook(success, user.userUUID, phone.phoneUUID, ticketUUID);
        callStripeWebhook(success, user.userUUID, phone.phoneUUID, ticketUUID);
      } else {
        callStripeWebhook(success, user.userUUID, phone.phoneUUID, ticketUUID);
      }
    }

    webhookCalled = true;
    continue;
  }

  if (status == "WAITING_LIST") {
    sleep(1);
    continue;
  }

  if (status == "SOLD_OUT") {
    soldOut.add(1);
    break;
  }

  if (status == "FAILED") {
    failedPayment.add(1);
    break;
  }

  if (status == "PAID") {
    successfulPayment.add(1);
    break;
  }

  sleep(2);
}
  sleep(1);
}

export function handleSummary(data) {
  const paid = data.metrics.Paid?.values?.count || 0;
  const failed = data.metrics.Failed?.values?.count || 0;
  const soldOut = data.metrics.SoldOut?.values?.count || 0;
  const badUserInput = data.metrics.BadUserInput?.values?.count || 0;

      console.log(`paid: ${paid}`);
      console.log(`failed: ${failed}`);
      console.log(`soldOut: ${soldOut}`)
      console.log(`badUserInput: ${badUserInput}`)
    
      if (paid <= 10) {
        console.log(`PASS: Less than 10 payments`);
      } else {
        console.error(`FAIL: Expected 10 or less payments, got ${paid}`);
      }
    
      if (failed + soldOut + badUserInput == options.vus - paid) {
        console.log(`PASS: Correct failed and soldout count`);
      } else {
        console.error(`FAIL: expected ${options.vus - paid} but got failed=${failed} soldOut=${soldOut} total=${failed + soldOut}`);
      }
    
  
  
    return {};
}

export function teardown() {
  http.post(`http://localhost:8080/reset`);
}

// Helper functions
function callStripeWebhook(success, userUUID, phoneUUID, ticketUUID) {
  const payload = JSON.stringify({
    userUUID: userUUID,
    phoneUUID: phoneUUID,
    ticketUUID: ticketUUID,
    paymentStatus: success ? "paid" : "unpaid",
  });

  return http.post('http://localhost:8080/stripeWebhookTest', payload, {
    headers: { 'Content-Type': 'application/json' },
  });
}
