import http from 'k6/http';
import {sleep } from 'k6';
import { Counter } from 'k6/metrics'

export const options = {
  vus: 100,
  iterations: 100,
};

export function setup() {
  const users = http.get('http://localhost:8080/users').json();
  const phones = http.get('http://localhost:8080/phones').json();
  //shorthand for { users: users, phones: phones }
  return { users, phones };
}

const successfulPayment = new Counter('Paid');
const failedPayment = new Counter('Failed')
const soldOut = new Counter('SoldOut')


export default function (data) {
    //make reservations
    const start = Date.now();
      const user = data.users[Math.floor(Math.random() * data.users.length)];
      const phone = data.phones[0];
    
      const payload = JSON.stringify({
        userUUID: user.userUUID,
        phoneUUID: phone.phoneUUID,
      });
    
      const createRes = http.post('http://localhost:8080/buy-request', payload, {
        headers: { 'Content-Type': 'application/json' },
      });
    
      if (createRes.status !== 200) {
        return;
      }
    const ticketUUID = createRes.json('ticketUUID');
    let webhookCalled = false

    //out of those user who get reservations, some call stripe success webhook, some failed
    while (true) {
        const statusRes = http.get(`http://localhost:8080/status/${ticketUUID}`);
        const status = statusRes.json('status');
        //counter sucesful reservation
        if (status == "SUCCESSFUL_RESERVATION") {
          //call stripe success webhook for some and failure for some
          if (!webhookCalled) {
            callStripeWebhook(Math.random() < 0.8, user.userUUID, phone.phoneUUID, ticketUUID)
          }
          webhookCalled = true
          continue;
        }
        //counter waiting list
        if (status == "WAITING_LIST") {
          continue;
        }

        if (status == "SOLD_OUT") {
          soldOut.add(1)
          break;
        }

        if (status == "FAILED") {
          failedPayment.add(1)
          break;
        }
    
        if (status == "PAID") {
          successfulPayment.add(1)
          break;
        }
        sleep(2); // Wait 1 second before checking again
      }
      sleep(10)
}


export function handleSummary(data) {
  //in the end we should have <10 succesful payements, rest sold out, and failed for those who got payment successful but then rollbacked

    const paid = data.metrics.Paid?.values?.count || 0;
    const failed = data.metrics.Failed?.values?.count || 0;
    const soldOut = data.metrics.SoldOut?.values?.count || 0;
    
  
    console.log(`paid: ${paid}`);
    console.log(`failed: ${failed}`);
    console.log(`soldOut: ${soldOut}`)
  
    if (paid <= 10) {
      console.log(`PASS: Less than 10 payments`);
    } else {
      console.error(`FAIL: Expected 10 or less payments, got ${paid}`);
    }
  
    if (failed + soldOut == options.vus - paid) {
      console.log(`PASS: Correct failed and soldout count`);
    } else {
      console.error(`FAIL: expected ${options.vus - paid} but got failed=${failed} soldOut=${soldOut} total=${failed + soldOut}`);
    }
  


  return {};
}

export function teardown() {
  const resetRes = http.post(`http://localhost:8080/reset`);
}


//helper functions
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