import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 10,
  iterations: 10,
};

export function setup() {
  const users = http.get('http://localhost:8080/users').json();
  const phones = http.get('http://localhost:8080/phones').json();
  return { users, phones };
}

//test that 10 users get successful reservation, others get waiting list
//instead of console logs use checks
export default function (data) {
  const user = data.users[Math.floor(Math.random() * data.users.length)];
  const phone = data.phones[0];

  const payload = JSON.stringify({
    userUUID: user.userUUID,
    phoneUUID: phone.phoneUUID,
  });

  const createRes = http.post('http://localhost:8080/buy-request', payload, {
    headers: { 'Content-Type': 'application/json' },
  });


  // 3. Verify Success
  if (createRes.status !== 200) {
    console.error(`FAILED: Status ${createRes.status}. Body: ${createRes.body}`);
    return;
  }

  const ticketUUID = createRes.json('ticketUUID');
  console.log(`SUCCESS: Got Ticket ID: ${ticketUUID}`);

  // 4. Polling Loop
  let isPending = true;
  for (let i = 0; i < 15; i++) {
    const statusRes = http.get(`http://localhost:8080/status/${ticketUUID}`);
    const status = statusRes.json('status');

    console.log(`Poll Attempt ${i + 1}: Current Status is "${status}"`);

    if (status !== 'PENDING' && status !== undefined) {
      console.log(`🎉 Ticket finished with status: ${status}`);
      isPending = false;
      break;
    }

    sleep(1); // Wait 1 second before checking again
  }

  if (isPending) {
    console.warn("TIMED OUT: Ticket stayed PENDING for too long.");
  }
}

export function teardown() {
 console.log("--- TEST FINISHED: Cleaning up Redis ---");
  const resetRes = http.post(`http://localhost:8080/reset`);
  if (resetRes.status === 200) {
    console.log("Redis reset successful.");
  } else {
    console.error(`Reset failed with status: ${resetRes.status}`);
  }
}