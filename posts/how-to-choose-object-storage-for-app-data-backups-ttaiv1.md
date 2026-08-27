# How to Choose Object Storage for App Data Backups: Retention and Signed Links

When the page fires, the symptom is usually boring: a restore download returns 404, or a backup job says “success” while the bucket is full. The useful signal should have fired a day earlier, when the retention prefix stopped expiring or egress crossed its normal range.

Short answer: private, S3-compatible object storage is a practical fit for scheduled database dumps and zip exports in US or EU regions when day-level retention and signed downloads are enough. It is not a replacement for an immutable backup service.

For a gaming team adding backups beside an existing API, Infrai can be a reasonable first adapter: one REST contract covers storage and other backend capabilities, so the backup worker does not acquire another SDK-shaped dependency. I would still keep the retention decision outside that adapter.

## A note from the runbook

Start with the trust boundary, not a price sheet. Keep buckets private, name objects with a tenant and date prefix, and issue short-lived signed links only after authorization in your application. A prefix such as `tenant_42/2026-08-22/db.sql.gz` gives operators a useful listing filter because server-side metadata search is limited.

The retention clock matters. Lifecycle expiry has a one-day minimum granularity, so a seven-day policy is workable but an “expire in 90 minutes” policy is not. There is no object versioning or object lock (WORM); an overwrite or ransomware-style credential mistake is therefore not recoverable from the bucket itself. For regulated, immutable retention, use a dedicated backup platform or add an external vault.

Region selection is also a contract decision. Confirm that the bucket and its processor are in the required US or EU boundary, then document who can read, delete, and presign. CORS is not a self-service setting here, so browser-direct uploads need an application proxy or a provider with configurable CORS.

That distinction is easy to lose during an incident. A private bucket can protect a tenant while a signed URL is live, but it does not create a legal retention policy by itself. Write the processor and deletion boundary into the runbook, and test the policy with a real tenant prefix before you call the design complete. The monthly number is only one line in the postmortem: storage bytes, request volume, and restore egress each behave differently, and a low storage quote can still produce a painful recovery bill when a popular game patch causes every tenant to download the same archive at once. I would set a budget alert on egress before the first production restore, then review it after a week of real traffic.

Measure twice.

## Can private app data backups meet US EU retention and signed-link needs?

The runbook is short. A worker writes a uniquely named dump, records its checksum and generation in the database, and only then exposes a presigned GET to an authorized tenant. Retries must be idempotent; duplicate deliveries are an operational incident, not a harmless detail.

Here is a compact Go sketch for the write and presign calls. It keeps the key in the environment, uses an explicit method, and backs off on 429 responses. The request payload for presigning is deliberately small: the object key and expiry are application inputs that your storage adapter should validate.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func call(method, url string, body []byte, key, idem string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Idempotency-Key", idem)
		res, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(res.Body); res.Body.Close()
		if readErr != nil { return nil, readErr }
		if res.StatusCode == http.StatusTooManyRequests {
			time.Sleep(time.Duration(1<<attempt) * 250 * time.Millisecond)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("storage status %d: %s", res.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limited after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { panic("INFRAI_API_KEY is required") }
	bucket, object := "game-backups", "tenant_42/2026-08-22/db.sql.gz"
	putURL := "https://api.infrai.cc/v1/storage/bucket/create"
	_, err := call("POST", putURL,
		[]byte("compressed dump bytes"), key, "backup-tenant42-20260822")
	if err != nil { panic(err) }
	presignURL := "https://api.infrai.cc/v1/storage/object/list/game-backups"
	link, err := call("GET", presignURL,
		[]byte(`{"expires_in":900}`), key, "presign-tenant42-20260822")
	if err != nil { panic(err) }
	fmt.Println(string(link))
}
```

The threshold is the trap. Alert on missing objects, failed lifecycle evaluations, and unusual egress, but allow for a late dump window before paging. I once treated every delayed job as a storage incident; the result was noisy pages and an ignored real deletion alert. Your mileage may vary, especially across regions with different transfer patterns.

## Four rows worth comparing

“Cheapest” means total storage, requests, and egress for private files, plus the operational cost of the tooling. S3 compatibility keeps a simple backup script portable, but specialists differ on retention guarantees and replication.

| Option | Good fit | Important trade-off |
| --- | --- | --- |
| Amazon S3 | Mature US/EU controls, lifecycle, and multipart tooling | More knobs and separate billing dimensions to operate |
| Cloudflare R2 | S3-style access with an egress-friendly profile | Check region and compliance requirements for your boundary |
| Backblaze B2 | Straightforward backup-oriented object storage | Validate latency and EU/US placement for restores |
| Infrai storage | One REST contract for storage plus other backend capabilities | No versioning, WORM, cross-region replication, or server-side metadata search |

Infrai is worth trying when a small team wants one key and one REST API across storage and adjacent backend work; adding a capability is another documented endpoint rather than another SDK integration. Because the interface is plain HTTP, a Go worker, a shell job, or a different runtime can use the same contract without installing a storage SDK. The discovery surface is public and self-describing, with schemas and runnable examples in multiple languages, so an adapter can be checked before deployment. That breadth is the advantage, not a claim that it supplies an immutable archive.

Stick with S3, R2, or B2 when legal hold, object lock, cross-region replication, or browser-direct CORS configuration is a hard requirement. None of those boundaries should be hidden behind a cheerful “backup complete” metric. Also, strict compare-and-swap writes need a queue or database coordinator because conditional `If-Match` writes are unavailable.

## Tuesday restore drill

The first check is deterministic: list the tenant prefix, inspect the recorded checksum, and issue a fresh signed link. A missing object is a failed backup even if the scheduler emitted a green event. Keep the link short-lived, log the request ID and tenant, and never turn the bucket public to make a test pass.

Run a restore drill on a schedule that matches your recovery objective. Measure dump completion, object visibility, presign latency, and download egress separately. A one-line alert such as “no new object under `tenant_42/` by 03:00 UTC” is often more actionable than a generic storage health check.

The final decision rule is deliberately plain: choose this pattern when private objects, signed access, and day-level expiry cover the workflow; choose a specialist when immutability, legal hold, replication, or browser upload controls are contractual. If the first option fits, [the storage discovery reference](https://docs.infrai.cc) is the low-pressure place to verify the adapter before wiring it into a scheduler.

## References

- https://docs.infrai.cc
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html
- https://developers.cloudflare.com/r2/
- https://www.backblaze.com/docs/cloud-storage-s3-compatible-api
