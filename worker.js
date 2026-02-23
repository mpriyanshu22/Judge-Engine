// worker.js

// BullMQ-based worker that runs untrusted code inside Docker using dockerode.



const { Worker } = require('bullmq');

const IORedis = require('ioredis');

const Docker = require('dockerode');

const fs = require('fs').promises;

const path = require('path');

const os = require('os');



// ---- Redis / BullMQ setup ----



const QUEUE_NAME = process.env.QUEUE_NAME || 'submissions';



// ---- Updated Redis / BullMQ setup in worker.js ----



// const redisConnection = new IORedis({

//   host: process.env.REDIS_HOST || 'redis-15072.c12.us-east-1-4.ec2.cloud.redislabs.com',

//   port: Number(process.env.REDIS_PORT) || 15072,

//   username: process.env.REDIS_USERNAME || 'default',

//   password: process.env.REDIS_PASSWORD || '9gpSO1axx9Q5QWOKJ3owi0cA9cs8ZAXw',

//   maxRetriesPerRequest: null,

//   enableReadyCheck: true,

// });



const redisConnection = new IORedis({

  host: '127.0.0.1', // ALWAYS use 127.0.0.1 for local testing

  port: 6379,

  maxRetriesPerRequest: null,

  enableReadyCheck: true,

});



const worker = new Worker(

  QUEUE_NAME,

  async (job) => {

    console.log(`[worker] 🎯 PICKED UP JOB: ${job.id}`);

    

    return await processSubmission(job);

  },

  {

    connection: redisConnection,

    // Increase lockDuration so long-running Docker startups don't lose the lock.

    lockDuration: 120000

  }

);



worker.on('active', (job) => {

  console.log(`[worker] 🔄 Job ${job.id} is now active on queue "${QUEUE_NAME}"`);

});



worker.on('completed', (job, result) => {

  console.log("-----------------------------------------");

  console.log(`✅ JOB FINISHED: ${job.id}`);

  console.log(`📊 STATUS: ${result.status}`);

  console.log(`⏱️  TIME: ${result.execution_time}ms`);

  if (result.output) console.log(`📄 OUTPUT: "${result.output}"`);

  if (result.stderr) console.log(`❗ ERROR: "${result.stderr}"`);

  console.log("-----------------------------------------");

});



worker.on('failed', (job, err) => {

  console.error(`[worker] ❌ Job ${job?.id} failed with error:`, err.message);

});



// ---- Docker setup ----



// On Windows, Docker Desktop usually listens on the named pipe: //./pipe/docker_engine

// On Linux/macOS, it uses /var/run/docker.sock

const isWindows = os.platform() === 'win32';

const docker = new Docker(isWindows ? { socketPath: '//./pipe/docker_engine' } : { socketPath: '/var/run/docker.sock' });



const EXECUTION_TIMEOUT_MS = Number(process.env.EXECUTION_TIMEOUT_MS) || 5000; // Increased for Docker on Windows 



// ---- Core job handler ----



async function processSubmission(job) {

  const {

    language = 'node',

    sourceCode,

    stdin = '',

    expectedOutput,

  } = job.data || {};



  if (!sourceCode || typeof sourceCode !== 'string') {

    return { status: 'Runtime Error', output: '', stderr: 'sourceCode is required', execution_time: 0 };

  }



  // Map incoming language IDs to internal docker logic

  const langKey = language.toLowerCase();

  if (langKey !== 'node' && langKey !== 'javascript') {

    return { status: 'Runtime Error', output: '', stderr: `Unsupported language: ${language}`, execution_time: 0 };

  }



  // Use the system temp directory to avoid permission issues

  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'judge-'));

  const codeFilename = 'solution.js';

  const codePath = path.join(tempDir, codeFilename);



  try {

    await fs.writeFile(codePath, sourceCode, 'utf8');



    const result = await runInDocker({

      sourceCode,

      codeFilename,

      stdin,

      expectedOutput,

    });



    console.log('[DEBUG] Full Result:', JSON.stringify(result, null, 2));

    return result;

  } catch (err) {

    console.error('[worker] Error during execution:', err);

    return {

      status: 'Runtime Error',

      output: '',

      stderr: String(err.message || err),

      execution_time: 0,

    };

  } finally {

    try {

      await fs.rm(tempDir, { recursive: true, force: true });

    } catch (cleanupErr) {

      console.error('[worker] Failed to clean temp dir:', cleanupErr.message);

    }

  }

}



// ---- Docker execution with sandboxing ----



async function runInDocker({ sourceCode, codeFilename, stdin, expectedOutput }) {

  const image = 'node:18-alpine';



  let container;

  const startTime = Date.now();

  let timedOut = false;

  let exitCode = null;

  let stdout = '';

  let stderr = '';



  try {

    // Use a long-lived container plus Docker Exec for more reliable stdin/stdout handling:

    // 1) Start a container that simply sleeps.

    // 2) Use exec to write USER_CODE (base64-encoded) to /tmp/solution.js.

    // 3) Use a second exec, with attached stdio, to run `node /tmp/solution.js`.



    // Base64-encode sourceCode to safely pass it through environment variables and shell.

    const sourceCodeBase64 = Buffer.from(sourceCode, 'utf8').toString('base64');



    // Step 1: create a long-lived container.

    container = await docker.createContainer({

      Image: image,

      Cmd: ['sleep', '60'],

      Tty: false,

      OpenStdin: false,

      StdinOnce: false,

      HostConfig: {

        NetworkMode: 'none',

        Memory: 512 * 1024 * 1024,

        NanoCPUs: 1 * 1e9,

      },

    });



    await container.start();



    // Step 2: write the code into /tmp/solution.js using an exec.

    const writeExec = await container.exec({

      Cmd: ['sh', '-c', 'echo "$USER_CODE" | base64 -d > /tmp/solution.js'],

      Env: [`USER_CODE=${sourceCodeBase64}`],

      AttachStdout: true,

      AttachStderr: true,

    });



    await new Promise((resolve, reject) => {

      writeExec.start({ hijack: true, stdin: false }, (err, execStream) => {

        if (err) return reject(err);



        execStream.on('end', resolve);

        execStream.on('error', reject);

      });

    });



    // Step 3: run the code with attached stdin/stdout/stderr.

    const runExec = await container.exec({

      Cmd: ['node', '/tmp/solution.js'],

      AttachStdin: true,

      AttachStdout: true,

      AttachStderr: true,

    });



    const runPromise = new Promise((resolve, reject) => {

      runExec.start({ hijack: true, stdin: true }, (err, execStream) => {

        if (err) return reject(err);



        // Send stdin if provided, then close the stream.

        if (stdin) {

          execStream.write(stdin);

        }

        execStream.end();



        execStream.on('end', async () => {

          try {

            const inspectData = await runExec.inspect();

            if (typeof inspectData.ExitCode === 'number') {

              exitCode = inspectData.ExitCode;

            }

          } catch (inspectErr) {

            console.error('[worker] Failed to inspect exec:', inspectErr.message);

            if (exitCode === null) exitCode = -1;

          }

          resolve();

        });



        execStream.on('error', (streamErr) => {

          reject(streamErr);

        });

      });

    });



    // Enforce an overall execution timeout.

    let timeoutId;

    const timeoutPromise = new Promise((resolve) => {

      timeoutId = setTimeout(async () => {

        timedOut = true;

        try {

          await container.stop({ t: 0 });

        } catch (stopErr) {

          // Ignore "no such container" errors to avoid noisy logs when the

          // container has already exited and been removed.

          if (!stopErr || stopErr.statusCode !== 404) {

            console.error('[worker] Failed to stop container on timeout:', stopErr.message);

          }

        }

        resolve();

      }, EXECUTION_TIMEOUT_MS);

    });



    await Promise.race([runPromise, timeoutPromise]);



    // If we finished before the timeout fired, cancel the timer so it doesn't

    // try to stop an already-removed container later.

    if (!timedOut && timeoutId) {

      clearTimeout(timeoutId);

    }



    const executionTime = Date.now() - startTime;



    // After exec completes, capture container logs and parse stdout/stderr

    try {

      const rawLogs = await container.logs({

        stdout: true,

        stderr: true,

        timestamps: false,

      });



      let logs;

      if (Buffer.isBuffer(rawLogs)) {

        logs = rawLogs;

      } else if (rawLogs && typeof rawLogs.on === 'function') {

        logs = await new Promise((resolve, reject) => {

          const chunks = [];

          rawLogs.on('data', (chunk) => chunks.push(chunk));

          rawLogs.on('end', () => resolve(Buffer.concat(chunks)));

          rawLogs.on('error', reject);

        });

      }



      if (logs && logs.length > 0) {

        let offset = 0;

        const stdoutChunks = [];

        const stderrChunks = [];



        while (offset < logs.length) {

          // Need at least 8 bytes for header

          if (logs.length - offset < 8) {

            // Remaining bytes without header - treat as stdout

            if (logs.length > offset) {

              stdoutChunks.push(logs.slice(offset));

            }

            break;

          }



          const streamType = logs[offset];

          const length = logs.readUInt32BE(offset + 4);

          offset += 8;



          // Check if we have complete data

          if (offset + length > logs.length) {

            // Incomplete chunk - skip to avoid corruption

            break;

          }



          // Extract data payload (skip the 8-byte header)

          const data = logs.slice(offset, offset + length);

          offset += length;



          // Stream type 1 = stdout, 2 = stderr

          if (streamType === 1) {

            stdoutChunks.push(data);

          } else if (streamType === 2) {

            stderrChunks.push(data);

          }

          // Ignore other stream types (stdin = 0)

        }



        stdout = Buffer.concat(stdoutChunks).toString('utf8').trim();

        stderr = Buffer.concat(stderrChunks).toString('utf8');

      }

    } catch (logErr) {

      console.error('[worker] Failed to get container logs:', logErr.message);

    }



    // If exec timed out and we never got an exit code, mark accordingly.

    if (timedOut && exitCode === null) {

      exitCode = -1;

      if (!stderr) {

        stderr = `Execution timed out after ${EXECUTION_TIMEOUT_MS}ms.`;

      }

    }



    // If exitCode !== 0 but stderr is empty, populate it with error information.

    if (exitCode !== 0 && exitCode !== null && !stderr) {

      stderr = `Process exited with code ${exitCode}. No error output captured.`;

      if (stdout) {

        stderr += ` Stdout: ${stdout}`;

      }

    }



    // If exitCode is still null (unexpected), infer from stderr.

    if (exitCode === null) {

      if (stderr) {

        exitCode = 1; // Assume error if stderr has content

      } else {

        exitCode = 0; // Assume success if no stderr

      }

    }



    let status = 'Accepted';

    if (timedOut) status = 'TLE';

    else if (exitCode !== 0) status = 'Runtime Error';

    else if (expectedOutput) {

        status = stdout.trim() === expectedOutput.trim() ? 'Accepted' : 'Wrong Answer';

    }



    return { status, output: stdout, stderr, execution_time: executionTime };

  } catch (err) {

    console.error('[worker] Error in runInDocker:', err);

    const executionTime = Date.now() - startTime;

    return {

      status: 'Runtime Error',

      output: '',

      stderr: String(err.message || err),

      execution_time: executionTime,

    };

  } finally {

    if (container) {

      try {

        await container.remove({ force: true });

      } catch (err) {

        console.error('[worker] Failed to remove container:', err.message);

      }

    }

  }

}



// ---- Graceful shutdown ----



async function shutdown() {

  console.log('\n[worker] 🛑 Shutting down gracefully...');

  await worker.close();

  await redisConnection.quit();

  process.exit(0);

}



process.on('SIGINT', shutdown);

process.on('SIGTERM', shutdown);



// ---- Startup Log ----

console.log("-----------------------------------------");

console.log(`🚀 Judge Engine Worker started successfully!`);

console.log(`📡 Listening on Queue: "${QUEUE_NAME}"`);

console.log(`🐳 Docker Socket: ${isWindows ? 'Windows Pipe' : '/var/run/docker.sock'}`);

console.log("-----------------------------------------");

