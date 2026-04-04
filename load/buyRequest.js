import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics'
import { Trend } from 'k6/metrics';
const vuDuration = new Trend('vu_duration');

export const options = {
  vus: 50,
  iterations: 50,
};

export function setup() {
  const users = http.get('http://localhost:8080/users').json();
  const phones = http.get('http://localhost:8080/phones').json();
  //shorthand for { users: users, phones: phones }
  return { users, phones };
}

const successfulReservation = new Counter('Reserved');
const waitingList = new Counter('WaitingList');

/**
 * 
 * sleep(1) inflates your response time metrics because it's included in the iteration duration.
  It is necessary for polling logic, but just be aware latency numbers in Grafana will reflect the sleep time too, not just actual request time data 
 *  
 */
export default function (data) {
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

  //Polling Loop
  for (let i = 0; i < 15; i++) {
    const statusRes = http.get(`http://localhost:8080/status/${ticketUUID}`);
    const status = statusRes.json('status');
    //counter sucesful reservation
    if (status == "SUCCESSFUL_RESERVATION") {
      successfulReservation.add(1)
      break;
    }
    //counter waiting list
    if (status == "WAITING_LIST") {
      waitingList.add(1)
      break;
    }

    sleep(0.1); // Wait 1 second before checking again
  }
  vuDuration.add(Date.now() - start); 
  //sleep(10)

}

//it runs once at the very end, after all iterations are done. That's where you get the final metric values.
//check() doesn't work in handleSummary — it's outside the VU context. console.error is the right way to assert here
export function handleSummary(data) {
  const successful = data.metrics.Reserved?.values?.count || 0;
  const waiting = data.metrics.WaitingList?.values?.count || 0;

  console.log(`Successful Reservations: ${successful}`);
  console.log(`Waiting List: ${waiting}`);

  if (successful !== 10) {
    console.error(`FAIL: Expected 10 successful reservations, got ${successful}`);
  } else {
    console.log(`PASS: Exactly 10 successful reservations`);
  }

  if (waiting !== options.iterations - 10) {
    console.error(`FAIL: Expected ${options.iterations - 10} waiting list, got ${waiting}`);
  } else {
    console.log(`PASS: Correct waiting list count`);
  }

  return {};
}

export function teardown() {
  const resetRes = http.post(`http://localhost:8080/reset`);
}