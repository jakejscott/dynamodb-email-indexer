import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: [
    { duration: "15s", target: 10 },
    { duration: "30s", target: 25 },
    { duration: "60s", target: 50 },
    { duration: "20s", target: 0 },
  ],
  thresholds: {
    // 95% of requests must finish within 100ms.
    http_req_duration: ["p(95) < 100"],
  },
  ext: {
    loadimpact: {
      distribution: {
        "amazon:au:sydney": { loadZone: "amazon:au:sydney", percent: 100 },
      },
    },
  },
};

export default function () {
  const url = __ENV.URL;
  const query = __ENV.QUERY;
  const limit = parseInt(__ENV.LIMIT);

  const payload = JSON.stringify({
    query: query,
    limit: limit,
  });

  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };

  const res = http.post(url, payload, params);
  check(res, { "status was 200": (r) => r.status == 200 });

  const json = res.json();
  check(json, {
    "no errors": (x) => x.error == null,
  });

  sleep(1);
}
