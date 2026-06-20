# Amazon Transcribe vs AmiVoice WebSocke 比較ハーネス

## asr-bench.ts

Amazon Transcribe vs AmiVoice WebSocket をスタンドアロンで比較するベンチマークハーネス。

### 前提

- Node.js 22+ (`Intl.Segmenter` を使う日本語トークナイズのため)
- `.env.local` に以下を設定:
  ```
  AWS_REGION=ap-northeast-1
  AWS_ACCESS_KEY_ID=...
  AWS_SECRET_ACCESS_KEY=...
  AMIVOICE_APPKEY=...
  ```
- 比較する音声は **16-bit PCM mono / 16kHz の WAV** であること (ヘッダで検証)

### 使い方

```bash
# 両プロバイダで1回ずつ
npx tsx scripts/asr-bench.ts \
  --audio samples/sample-1.wav \
  --reference samples/sample-1.txt \
  --providers transcribe,amivoice \
  --runs 1

# AmiVoiceだけ3回 (ばらつき確認)
npx tsx scripts/asr-bench.ts \
  --audio samples/sample-1.wav \
  --reference samples/sample-1.txt \
  --providers amivoice \
  --runs 3

# AmiVoice の単語登録 (profileWords) ありで実行
npx tsx scripts/asr-bench.ts \
  --audio samples/sample-3.wav \
  --reference samples/sample-3.txt \
  --providers amivoice \
  --amivoice-profile-words samples/sample-3.profile-words.txt \
  --out bench-result-sample-3-with-profile.md
```

`--reference` を渡すと WER / CER を計算する。なしでもフィラー出現数とレイテンシ・コストは出る。

`--amivoice-profile-words` は **セッション限定の単語登録ファイル** を指定する。1行1エントリで `表記 読み仮名` (半角スペース区切り)、`#` で始まる行はコメント扱い。スクリプト内で `|` 結合して `s` パケット (start command) に `profileWords="..."` として埋め込む。永続的な辞書登録ではなく、その接続だけ有効。

### 出力

- 標準出力に進捗ログ + 最終Markdown
- `bench-result-<audio名>.md` を生成 (記事に貼り付ける用)

### 計測指標

| 指標                | 算出方法                                                                                                                                                                                                                                                             |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| WER                 | 単語 (`Intl.Segmenter` の word) ベースの編集距離 / 参照長                                                                                                                                                                                                            |
| CER                 | 文字ベースの編集距離 / 参照長                                                                                                                                                                                                                                        |
| First partial (ms)  | 接続開始から最初の中間結果到着まで                                                                                                                                                                                                                                   |
| Tail avg / max (ms) | **発話単位の体感レイテンシ**。各発話について `(arrivedAt - 音声送信開始時刻) - 発話のendtime (音声内ms)` を計算し、平均と最大を出す。`+` は発話末から確定到着までの実待ち時間、`-` は発話末以前に届いた (理論上はあり得ないので、サーバが先回りして確定した稀ケース) |
| 発話数              | tail 計算に使った発話の数 (Transcribe: `EndTime` 付き final result、AmiVoice: A event の `endtime`)                                                                                                                                                                  |
| フィラー (ref→hyp)  | リファレンス側のフィラー数 → ASR出力側のフィラー数                                                                                                                                                                                                                   |
| 除去率              | `(ref - hyp) / ref` を 0〜1 にクランプして百分率化。負の値 (ASRがフィラーを増やす場合) は 0%                                                                                                                                                                         |
| Cost (¥)            | Transcribe: $0.024/min × 150円/$、AmiVoice: ¥79.2/h                                                                                                                                                                                                                  |

フィラー判定は `FILLER_PATTERNS` (長い順マッチ) と `SHORT_FILLER_REGEXES` (前後文字種境界つきの短い「ま」「あ」検出) の合算。完全一致ではなく文字列マッチなので、`あの人` の `あの` は誤検知しないが、`まあまあ` のような連続フィラーは1個と数えるなど近似がある。

### 注意事項

- 音声チャンクは100msずつ送信しリアルタイム配信を模擬する。バッチ最速送信ではない。
- AmiVoiceは `nolog` エンドポイント (`wss://acp-api.amivoice.com/v1/nolog/`) を使用 — ログ保存しない代わりに割引なし
- AmiVoiceのエンジンは `-a-general` 固定。
- フィラー除去 (`keepFillerToken=0`) はAmiVoice側でデフォルト有効。`keepFillerToken=1` を試したい場合は `s` パケットに追記
