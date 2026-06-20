/**
 * ASR Benchmark Harness — Amazon Transcribe vs AmiVoice
 *
 * Runs the same WAV file through both providers and reports WER/CER/latency/cost.
 * For the Zennfes Spring 2026 article comparing AmiVoice with AWS Transcribe.
 *
 * Usage:
 *   npx tsx scripts/asr-bench.ts \
 *     --audio samples/sample-1.wav \
 *     --reference samples/sample-1.txt \
 *     --providers transcribe,amivoice \
 *     --runs 1
 *
 * Required env (.env.local):
 *   AWS_REGION=ap-northeast-1
 *   AWS_ACCESS_KEY_ID=...
 *   AWS_SECRET_ACCESS_KEY=...
 *   AMIVOICE_APPKEY=...
 *
 * Audio format: 16-bit PCM mono, 16kHz. Header is parsed and stripped.
 */

import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { resolve, basename } from 'node:path';
import { performance } from 'node:perf_hooks';
import { config as loadEnv } from 'dotenv';
import {
  TranscribeStreamingClient,
  StartStreamTranscriptionCommand,
  type AudioStream,
} from '@aws-sdk/client-transcribe-streaming';
import WebSocket from 'ws';
import levenshtein from 'fast-levenshtein';

loadEnv({ path: '.env.local', override: true });

// .env.local provides static credentials. Drop any AWS_PROFILE inherited from
// the parent shell so the SDK uses the env vars instead of an expired profile.
if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
  delete process.env.AWS_PROFILE;
}

type Provider = 'transcribe' | 'amivoice';

interface CliArgs {
  audio: string;
  reference?: string;
  providers: Provider[];
  runs: number;
  out?: string;
  amivoiceProfileWords?: string;     // pipe-delimited "WRITTEN SPOKEN|WRITTEN SPOKEN|..." (already serialized)
  amivoiceProfileWordsPath?: string; // original file path for display
}

interface ProviderResult {
  provider: Provider;
  run: number;
  transcript: string;
  rawResponse: unknown;
  latencyMs: {
    firstPartial: number | null;          // ms from connection open to the first partial
    tailPerUtteranceAvg: number | null;   // mean tail latency per utterance (final arrived - audio-internal endtime)
    tailPerUtteranceMax: number | null;   // worst tail latency among utterances
    utteranceCount: number;               // how many utterances contributed to the average
  };
  audioDurationSec: number;
  estimatedCostJpy: number;
  error?: string;
}

interface AccuracyMetrics {
  wer: number | null;
  cer: number;
  fillerInRef: number | null;
  fillerInHyp: number;
  fillerRemovalRate: number | null;
}

const PCM_SAMPLE_RATE = 16_000;
const PCM_CHUNK_BYTES = 3200; // 100ms at 16kHz/16bit/mono

// Order matters: longer patterns first so partial overlaps don't double-count.
const FILLER_PATTERNS = [
  'えっとー', 'えーっと', 'えっと', 'えーと', 'えと',
  'あのー', 'あのう',
  'まあ', 'まー',
  'なんか',
  'えー、', 'えー',
  'うーん', 'うんうん',
  'あー', 'おー',
  'そのー',
];

// Short-form fillers ("ま" / "あ") often glue to the next content word.
// Match only when a clear boundary precedes (start, punctuation, space) AND
// a content character follows — protects "未来" / "あなた" / "アメリカ" etc.
const SHORT_FILLER_REGEXES: RegExp[] = [
  /(^|[、。\s「『（(])ま(?=[一-龯ァ-ヴA-Za-z0-9])/g,
  /(^|[、。\s「『（(])あ(?=[一-龯ァ-ヴA-Za-z0-9])/g,
  /(^|[、。\s「『（(])あの(?=[一-龯ァ-ヴA-Za-z0-9])/g,
];

const USD_TO_JPY = 150;
const TRANSCRIBE_USD_PER_MIN = 0.024;
const AMIVOICE_JPY_PER_HOUR = 79.2;

function parseArgs(argv: string[]): CliArgs {
  const args: Record<string, string> = {};
  for (let i = 2; i < argv.length; i += 2) {
    const key = argv[i]?.replace(/^--/, '');
    const value = argv[i + 1];
    if (!key || !value) {
      throw new Error(`Bad argument near "${argv[i]}"`);
    }
    args[key] = value;
  }

  if (!args.audio) {
    console.error('Usage: tsx scripts/asr-bench.ts --audio <wav> [--reference <txt>] [--providers transcribe,amivoice] [--runs 1] [--out <md>] [--amivoice-profile-words <txt>]');
    process.exit(1);
  }

  const providers = (args.providers ?? 'transcribe,amivoice')
    .split(',')
    .map((p) => p.trim()) as Provider[];

  for (const p of providers) {
    if (p !== 'transcribe' && p !== 'amivoice') {
      throw new Error(`Unknown provider: ${p}`);
    }
  }

  // Read profileWords from a file: one entry per line "WRITTEN SPOKEN" (space-separated).
  // Lines starting with `#` and blank lines are ignored. Joined with `|` for the s-packet.
  let amivoiceProfileWords: string | undefined;
  let amivoiceProfileWordsPath: string | undefined;
  const pwArg = args['amivoice-profile-words'];
  if (pwArg) {
    amivoiceProfileWordsPath = resolve(pwArg);
    const raw = readFileSync(amivoiceProfileWordsPath, 'utf-8');
    const entries = raw.split('\n')
      .map((l) => l.trim())
      .filter((l) => l && !l.startsWith('#'));
    if (entries.length === 0) {
      throw new Error(`profile-words file ${pwArg} contains no usable entries`);
    }
    amivoiceProfileWords = entries.join('|');
  }

  return {
    audio: resolve(args.audio),
    reference: args.reference ? resolve(args.reference) : undefined,
    providers,
    runs: args.runs ? Number(args.runs) : 1,
    out: args.out,
    amivoiceProfileWords,
    amivoiceProfileWordsPath,
  };
}

function readPcmFromWav(filePath: string): { pcm: Buffer; durationSec: number } {
  const buf = readFileSync(filePath);
  if (buf.toString('ascii', 0, 4) !== 'RIFF' || buf.toString('ascii', 8, 12) !== 'WAVE') {
    throw new Error(`${filePath} is not a RIFF/WAVE file`);
  }

  let offset = 12;
  let fmtChannels = 0;
  let fmtSampleRate = 0;
  let fmtBitsPerSample = 0;
  let dataOffset = -1;
  let dataLength = 0;

  while (offset < buf.length - 8) {
    const chunkId = buf.toString('ascii', offset, offset + 4);
    const chunkSize = buf.readUInt32LE(offset + 4);
    if (chunkId === 'fmt ') {
      fmtChannels = buf.readUInt16LE(offset + 10);
      fmtSampleRate = buf.readUInt32LE(offset + 12);
      fmtBitsPerSample = buf.readUInt16LE(offset + 22);
    } else if (chunkId === 'data') {
      dataOffset = offset + 8;
      dataLength = chunkSize;
      break;
    }
    offset += 8 + chunkSize;
  }

  if (dataOffset < 0) throw new Error('No data chunk found in WAV');
  if (fmtChannels !== 1) throw new Error(`Expected mono, got ${fmtChannels} channels`);
  if (fmtSampleRate !== PCM_SAMPLE_RATE) {
    throw new Error(`Expected ${PCM_SAMPLE_RATE}Hz, got ${fmtSampleRate}Hz`);
  }
  if (fmtBitsPerSample !== 16) throw new Error(`Expected 16-bit, got ${fmtBitsPerSample}-bit`);

  const pcm = buf.subarray(dataOffset, dataOffset + dataLength);
  const durationSec = pcm.length / 2 / PCM_SAMPLE_RATE;
  return { pcm, durationSec };
}

interface ChunkProgress {
  firstChunkSentAt: number | null;
}

async function* pcmChunkStream(pcm: Buffer, progress?: ChunkProgress): AsyncGenerator<Buffer> {
  for (let i = 0; i < pcm.length; i += PCM_CHUNK_BYTES) {
    if (i === 0 && progress) {
      // Stamp the moment audio starts flowing. This is the wall-clock anchor for
      // mapping audio-internal timestamps (endtime) back to wall-clock arrivals.
      progress.firstChunkSentAt = performance.now();
    }
    yield pcm.subarray(i, Math.min(i + PCM_CHUNK_BYTES, pcm.length));
    // Throttle to roughly real-time so APIs treat this like a live stream
    await new Promise((r) => setTimeout(r, 100));
  }
}

async function runTranscribe(pcm: Buffer, durationSec: number, run: number): Promise<ProviderResult> {
  const region = process.env.AWS_REGION ?? 'ap-northeast-1';
  const client = new TranscribeStreamingClient({ region });

  const startedAt = performance.now();
  let firstPartialAt: number | null = null;
  let finalText = '';
  const partialTexts: string[] = [];
  const utteranceTimings: UtteranceTiming[] = [];
  const progress: ChunkProgress = { firstChunkSentAt: null };

  const audioStream = async function* (): AsyncIterable<AudioStream> {
    for await (const chunk of pcmChunkStream(pcm, progress)) {
      yield { AudioEvent: { AudioChunk: new Uint8Array(chunk) } };
    }
  };

  const command = new StartStreamTranscriptionCommand({
    LanguageCode: 'ja-JP',
    MediaSampleRateHertz: PCM_SAMPLE_RATE,
    MediaEncoding: 'pcm',
    AudioStream: audioStream(),
  });

  try {
    const response = await client.send(command);
    if (!response.TranscriptResultStream) {
      throw new Error('No TranscriptResultStream in response');
    }

    for await (const event of response.TranscriptResultStream) {
      if (event.TranscriptEvent?.Transcript?.Results) {
        for (const result of event.TranscriptEvent.Transcript.Results) {
          const text = result.Alternatives?.[0]?.Transcript ?? '';
          if (!text) continue;
          if (firstPartialAt === null) firstPartialAt = performance.now();
          if (result.IsPartial) {
            // overwrite — partials are cumulative within an utterance
          } else {
            partialTexts.push(text);
            // Transcribe Streaming returns EndTime in seconds (audio-internal)
            const endTimeSec = typeof result.EndTime === 'number' ? result.EndTime : null;
            if (endTimeSec !== null) {
              utteranceTimings.push({ endtimeMs: endTimeSec * 1000, arrivedAt: performance.now() });
            }
          }
        }
      }
    }

    finalText = partialTexts.join(' ').trim();

    const minutes = durationSec / 60;
    const costJpy = Math.round(TRANSCRIBE_USD_PER_MIN * minutes * USD_TO_JPY * 100) / 100;
    const tailStats = computeTailStats(utteranceTimings, progress.firstChunkSentAt);

    return {
      provider: 'transcribe',
      run,
      transcript: finalText,
      rawResponse: { partialTexts, utteranceTimings },
      latencyMs: {
        firstPartial: firstPartialAt !== null ? firstPartialAt - startedAt : null,
        tailPerUtteranceAvg: tailStats.avg,
        tailPerUtteranceMax: tailStats.max,
        utteranceCount: tailStats.count,
      },
      audioDurationSec: durationSec,
      estimatedCostJpy: costJpy,
    };
  } catch (err) {
    return {
      provider: 'transcribe',
      run,
      transcript: '',
      rawResponse: null,
      latencyMs: { firstPartial: null, tailPerUtteranceAvg: null, tailPerUtteranceMax: null, utteranceCount: 0 },
      audioDurationSec: durationSec,
      estimatedCostJpy: 0,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

async function runAmiVoice(pcm: Buffer, durationSec: number, run: number, profileWords?: string): Promise<ProviderResult> {
  const appkey = process.env.AMIVOICE_APPKEY;
  if (!appkey) {
    return {
      provider: 'amivoice',
      run,
      transcript: '',
      rawResponse: null,
      latencyMs: { firstPartial: null, tailPerUtteranceAvg: null, tailPerUtteranceMax: null, utteranceCount: 0 },
      audioDurationSec: durationSec,
      estimatedCostJpy: 0,
      error: 'AMIVOICE_APPKEY env var not set',
    };
  }

  const endpoint = 'wss://acp-api.amivoice.com/v1/nolog/';
  const engine = '-a-general';
  const audioFormat = 'LSB16K'; // Int16 LE 16kHz

  return new Promise((resolvePromise) => {
    const ws = new WebSocket(endpoint);
    const startedAt = performance.now();
    let firstPartialAt: number | null = null;
    const progress: ChunkProgress = { firstChunkSentAt: null };
    const utterances: string[] = [];
    const utteranceTimings: UtteranceTiming[] = [];
    const rawEvents: unknown[] = [];

    const finalize = (errMsg?: string) => {
      const hours = durationSec / 3600;
      const costJpy = Math.round(AMIVOICE_JPY_PER_HOUR * hours * 100) / 100;
      const tailStats = computeTailStats(utteranceTimings, progress.firstChunkSentAt);
      try { ws.close(); } catch { /* ignore */ }
      resolvePromise({
        provider: 'amivoice',
        run,
        transcript: utterances.join(' ').trim(),
        rawResponse: rawEvents,
        latencyMs: {
          firstPartial: firstPartialAt !== null ? firstPartialAt - startedAt : null,
          tailPerUtteranceAvg: tailStats.avg,
          tailPerUtteranceMax: tailStats.max,
          utteranceCount: tailStats.count,
        },
        audioDurationSec: durationSec,
        estimatedCostJpy: costJpy,
        error: errMsg,
      });
    };

    ws.on('open', async () => {
      // s packet: TEXT message starting the session.
      // profileWords is a session-only custom dictionary: "WRITTEN SPOKEN|WRITTEN SPOKEN|...".
      // The whole value is wrapped in double quotes per AmiVoice docs.
      const profileWordsArg = profileWords ? ` profileWords="${profileWords}"` : '';
      const sCommand = `s ${audioFormat} ${engine}${profileWordsArg} authorization=${appkey}`;
      ws.send(sCommand);

      try {
        for await (const chunk of pcmChunkStream(pcm, progress)) {
          if (ws.readyState !== WebSocket.OPEN) break;
          // p packet: BINARY message prefixed with 0x70 ('p') + raw PCM
          const pPacket = Buffer.concat([Buffer.from([0x70]), chunk]);
          ws.send(pPacket);
        }
        if (ws.readyState === WebSocket.OPEN) {
          ws.send('e'); // end command
        }
      } catch (err) {
        finalize(err instanceof Error ? err.message : String(err));
      }
    });

    ws.on('message', (data) => {
      const text = data.toString('utf-8');
      // AmiVoice events: "s", "p", "e" responses, plus "U "/"A " events with JSON payload
      const eventType = text[0];
      const payload = text.slice(2);
      if (eventType === 'U') {
        if (firstPartialAt === null) firstPartialAt = performance.now();
        try { rawEvents.push({ type: 'U', json: JSON.parse(payload) }); } catch { /* ignore */ }
      } else if (eventType === 'A') {
        try {
          const parsed = JSON.parse(payload);
          rawEvents.push({ type: 'A', json: parsed });
          if (parsed.text) {
            utterances.push(parsed.text);
            // AmiVoice: per-utterance endtime lives in results[0].endtime (audio-internal ms).
            const endtime = Array.isArray(parsed.results) && typeof parsed.results[0]?.endtime === 'number'
              ? parsed.results[0].endtime
              : null;
            if (endtime !== null) {
              utteranceTimings.push({ endtimeMs: endtime, arrivedAt: performance.now() });
            }
          }
        } catch { /* ignore */ }
      } else if (eventType === 'e') {
        // end ack — wait for socket close
      }
    });

    ws.on('close', () => finalize());
    ws.on('error', (err) => finalize(err.message));
  });
}

interface UtteranceTiming {
  endtimeMs: number;        // audio-internal time when the utterance ended (ms from audio start)
  arrivedAt: number;        // wall-clock time when this utterance's final transcript was received
}

interface TailStats {
  avg: number | null;
  max: number | null;
  count: number;
}

/**
 * Per-utterance tail latency:
 *   tail_i = (arrivedAt_i - audioStartedAt) - endtimeMs_i
 * That is: how long after the audio bytes for utterance i finished playing
 * did the final transcript for utterance i arrive on the wire?
 *
 * Returns null avg/max if no usable utterance timings are available.
 */
function computeTailStats(timings: UtteranceTiming[], audioStartedAt: number | null): TailStats {
  if (!audioStartedAt || timings.length === 0) {
    return { avg: null, max: null, count: 0 };
  }
  const tails = timings.map((t) => (t.arrivedAt - audioStartedAt) - t.endtimeMs);
  const sum = tails.reduce((a, b) => a + b, 0);
  return {
    avg: sum / tails.length,
    max: Math.max(...tails),
    count: tails.length,
  };
}

function tokenizeJa(s: string): string[] {
  // Use Intl.Segmenter for word-level tokenization (Node 22+)
  if (typeof Intl !== 'undefined' && 'Segmenter' in Intl) {
    const segmenter = new Intl.Segmenter('ja', { granularity: 'word' });
    return Array.from(segmenter.segment(s))
      .filter((s) => s.isWordLike)
      .map((s) => s.segment);
  }
  return s.split(/\s+/).filter(Boolean);
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Count filler-like tokens in the text using two passes:
 *  1. Long-form patterns (FILLER_PATTERNS) are matched on a mask that hides
 *     already-counted positions, so "あのー" is not also counted as "あの".
 *  2. Short-form regexes ("ま" / "あ" before a content char) catch the
 *     glued-on cases that Transcribe leaves in the output.
 */
function countFillers(text: string): number {
  if (!text) return 0;

  // Use a mutable mask: replace counted spans with a placeholder so subsequent
  // patterns don't re-match the same characters.
  const chars = text.split('');
  let total = 0;

  for (const pattern of FILLER_PATTERNS) {
    const re = new RegExp(escapeRegex(pattern), 'g');
    let m: RegExpExecArray | null;
    const masked = chars.join('');
    re.lastIndex = 0;
    while ((m = re.exec(masked)) !== null) {
      const start = m.index;
      const end = start + m[0].length;
      // Skip if this span is already masked (any '\0' in the underlying chars)
      if (chars.slice(start, end).some((c) => c === '\0')) continue;
      total += 1;
      for (let i = start; i < end; i++) chars[i] = '\0';
    }
  }

  for (const re of SHORT_FILLER_REGEXES) {
    const masked = chars.join('');
    re.lastIndex = 0;
    let m: RegExpExecArray | null;
    while ((m = re.exec(masked)) !== null) {
      // m[0] starts with the boundary char (or empty for "^"); the filler
      // character itself is at position m.index + m[1].length.
      const fillerStart = m.index + (m[1]?.length ?? 0);
      const fillerEnd = m.index + m[0].length;
      if (chars.slice(fillerStart, fillerEnd).some((c) => c === '\0')) continue;
      total += 1;
      for (let i = fillerStart; i < fillerEnd; i++) chars[i] = '\0';
    }
  }

  return total;
}

function computeAccuracy(hyp: string, ref?: string): AccuracyMetrics {
  const fillerInHyp = countFillers(hyp);

  if (!ref) {
    return { wer: null, cer: 0, fillerInRef: null, fillerInHyp, fillerRemovalRate: null };
  }

  const fillerInRef = countFillers(ref);
  const fillerRemovalRate = fillerInRef === 0
    ? null
    : Math.max(0, Math.min(1, (fillerInRef - fillerInHyp) / fillerInRef));

  const refTokens = tokenizeJa(ref);
  const hypTokens = tokenizeJa(hyp);
  const wer = refTokens.length === 0
    ? 0
    : levenshtein.get(refTokens.join(' '), hypTokens.join(' ')) / Math.max(refTokens.join(' ').length, 1);

  const cer = ref.length === 0
    ? 0
    : levenshtein.get(ref, hyp) / ref.length;

  return { wer, cer, fillerInRef, fillerInHyp, fillerRemovalRate };
}

function formatMarkdown(audioPath: string, reference: string | undefined, results: Array<ProviderResult & AccuracyMetrics>, profileWordsPath?: string): string {
  const lines: string[] = [];
  lines.push(`# ASR Benchmark Result`);
  lines.push('');
  lines.push(`- **Audio**: \`${basename(audioPath)}\``);
  lines.push(`- **Reference**: ${reference ? `\`${basename(reference)}\`` : '_(none — only filler count is reliable)_'}`);
  if (profileWordsPath) {
    lines.push(`- **AmiVoice profileWords**: \`${basename(profileWordsPath)}\` (session-only custom dictionary)`);
  }
  lines.push('');
  lines.push('## Summary');
  lines.push('');
  lines.push('| Provider | Run | First partial (ms) | Tail avg / max (ms) | 発話数 | WER | CER | フィラー (ref→hyp) | 除去率 | Cost (¥) |');
  lines.push('|---|---|---|---|---|---|---|---|---|---|');
  for (const r of results) {
    const wer = r.wer !== null ? r.wer.toFixed(3) : 'n/a';
    const cer = r.cer.toFixed(3);
    const firstPartial = r.latencyMs.firstPartial !== null ? r.latencyMs.firstPartial.toFixed(0) : 'n/a';
    const fmt = (v: number | null) => v !== null ? (v >= 0 ? '+' : '') + v.toFixed(0) : 'n/a';
    const tail = r.latencyMs.tailPerUtteranceAvg !== null
      ? `${fmt(r.latencyMs.tailPerUtteranceAvg)} / ${fmt(r.latencyMs.tailPerUtteranceMax)}`
      : 'n/a';
    const fillerCol = r.fillerInRef !== null ? `${r.fillerInRef}→${r.fillerInHyp}` : `–→${r.fillerInHyp}`;
    const removal = r.fillerRemovalRate !== null ? `${(r.fillerRemovalRate * 100).toFixed(1)}%` : 'n/a';
    lines.push(
      `| ${r.provider} | ${r.run} | ${firstPartial} | ${tail} | ${r.latencyMs.utteranceCount} | ${wer} | ${cer} | ${fillerCol} | ${removal} | ${r.estimatedCostJpy.toFixed(2)} |`,
    );
  }
  lines.push('');
  lines.push('## Transcripts');
  lines.push('');
  if (reference) {
    lines.push('### Reference (人手書き起こし)');
    lines.push('');
    lines.push('```');
    lines.push(readFileSync(reference, 'utf-8').trim());
    lines.push('```');
    lines.push('');
  }
  for (const r of results) {
    lines.push(`### ${r.provider} (run ${r.run})`);
    lines.push('');
    if (r.error) {
      lines.push(`> Error: ${r.error}`);
      lines.push('');
      continue;
    }
    lines.push('```');
    lines.push(r.transcript || '_(empty)_');
    lines.push('```');
    lines.push('');
  }
  return lines.join('\n');
}

async function main() {
  const args = parseArgs(process.argv);

  if (!existsSync(args.audio)) {
    throw new Error(`Audio file not found: ${args.audio}`);
  }

  console.log(`[asr-bench] audio=${args.audio} providers=${args.providers.join(',')} runs=${args.runs}`);
  const { pcm, durationSec } = readPcmFromWav(args.audio);
  console.log(`[asr-bench] PCM ${pcm.length} bytes, ${durationSec.toFixed(2)}s`);

  const reference = args.reference ? readFileSync(args.reference, 'utf-8').trim() : undefined;

  const results: Array<ProviderResult & AccuracyMetrics> = [];
  for (let run = 1; run <= args.runs; run++) {
    for (const provider of args.providers) {
      console.log(`[asr-bench] -> ${provider} run ${run}`);
      const baseResult = provider === 'transcribe'
        ? await runTranscribe(pcm, durationSec, run)
        : await runAmiVoice(pcm, durationSec, run, args.amivoiceProfileWords);
      const accuracy = computeAccuracy(baseResult.transcript, reference);
      results.push({ ...baseResult, ...accuracy });
      if (baseResult.error) {
        console.error(`[asr-bench]    error: ${baseResult.error}`);
      } else {
        console.log(`[asr-bench]    transcript: ${baseResult.transcript.slice(0, 80)}...`);
      }
    }
  }

  const md = formatMarkdown(args.audio, args.reference, results, args.amivoiceProfileWordsPath);
  const outPath = args.out ?? `bench-result-${basename(args.audio).replace(/\.[^.]+$/, '')}.md`;
  writeFileSync(outPath, md, 'utf-8');
  console.log(`\n[asr-bench] wrote ${outPath}`);
  console.log('\n' + md);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
