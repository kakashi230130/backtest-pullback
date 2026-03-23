import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import pino from 'pino';
import pretty from 'pino-pretty';
import { config } from './config.js';

function ensureDir(dir) {
  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch {
    // ignore
  }
}

function dayStamp(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');
const outputDir = path.join(projectRoot, 'output');
ensureDir(outputDir);

const jobName = process.env.JOB_NAME || process.env.npm_lifecycle_event || 'app';
const logPath = path.join(outputDir, `${jobName}.${dayStamp()}.log`);

// Console (human readable)
const prettyConsole = pretty({ colorize: true, translateTime: 'SYS:standard' });

// File output (JSON, append)
const fileDest = pino.destination({ dest: logPath, mkdir: true, sync: false });

const streams = pino.multistream([
  { level: config.logLevel, stream: prettyConsole },
  { level: config.logLevel, stream: fileDest },
]);

export const logger = pino(
  {
    level: config.logLevel,
    base: null,
    timestamp: pino.stdTimeFunctions.isoTime,
  },
  streams,
).child({ job: jobName, pid: process.pid });
