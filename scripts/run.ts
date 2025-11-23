const CODE = `
output(await run("date"));
output(await run("date"));
output(await run("date"));
`;

const response = await fetch("http://localhost:8081/v1/stateless/run", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
  },
  body: JSON.stringify({
    image: "ubuntu:latest",
    instructions: CODE,
  }),
});

const result = await response.json();

console.log("results:\n", result);
