# Module 4C release evidence

This folder contains small sanitized receipts for the C8A synthetic responsiveness run, C8B bounded streaming run, and C9A-FIX3 validation. The original experiment reports remain under results/ and are intentionally not copied or rewritten here.

The current Module 4C ranking, publication, and evaluation specification is in
[module4-plan.md](../../module4-plan.md#module4c-spec);
current status and limitations are in [progress.md](../../progress.md#progress-module4c).
The retired standalone evaluation plan, release review, and C8B run procedure
are preserved as [evaluation-plan.md](evaluation-plan.md),
[release-review.md](release-review.md), and
[c8b-streaming-demo.md](c8b-streaming-demo.md).
The former `docs/module4c-evaluation-plan.md` path was a short
compatibility page because the C8A report generator includes that path in new
reports; the complete historical plan is preserved in this archive.

## Receipts

- [C8A responsiveness receipt](c8a-receipt.json) records the original report SHA-256, fixture scope, ranking response, and measured CLI timing.
- [C8B streaming receipt](c8b-receipt.json) records the original report SHA-256, event/watermark/window observations, serving checks, and the later cleanup correction.
- [C9A-FIX3 validation receipt](c9a-fix3-validation.json) records test counts, skipped categories, static checks, the concurrency-fix commit, and isolated Docker cleanup.

The original C8A report is results/module4c-responsiveness/c8a-20261013T144721Z-37027014/report.json. Its SHA-256 is cfb07b5d37fc244f9cbf9b5e7d22f4660ca3db47ec0f3f6352477718be095362.

The original C8B report is results/module4c-streaming-demo/c8b-20261013t153557z-11f0a25f/report.json. Its SHA-256 is 9035deca000e138840286514543769cfb11fe8680a5cf7c4c3c9fffc23523ecd. C8B's historical cleanup field predates the anonymous-volume correction in commit ace465107f40c4ead89834c347351a21ca738354. That report is preserved unchanged.

C8A and C8B are controlled synthetic demonstrations. They do not establish recommendation accuracy or production capacity.
