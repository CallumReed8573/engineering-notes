# Staging DNS Zones and Production Subdomains Explained for Write Boundary Blast Radius

Short answer: use a separate DNS zone for staging when a mistaken write must be unable to reach production; use a production subdomain when the same small team owns both and every change is reviewed. The decision is a write-boundary decision, not a naming preference.

I keep coming back to one operational question: who can publish a record at 02:00, and what can that credential address? A staging admin console that can select the production zone has a larger blast radius than its UI suggests. In an incident review, “the script was meant for staging” is not a control. The zone identifier is.

That distinction matters.

## Should staging DNS use a separate zone or production subdomain for write boundaries?

The separate-zone design makes the boundary physical. A bad script can remove or replace records in the staging zone, but it cannot delete production records that its credentials cannot address. That is the useful invariant when the console, deploy job, and on-call access are shared across environments.

A subdomain, such as `staging.example.com`, is a narrower change in inventory. The team keeps one zone, one set of records to discover, and less administrative work. That can be the right call when nobody is dedicated to DNS and a second zone would become abandoned paperwork. The catch is that the writer still has access to the parent zone; a bug in selection or authorization can cross the intended environment boundary.

Either design should store an explicit zone identifier per environment. Do not derive it from the string `staging`; names get renamed, and a derived lookup can silently point at the wrong account. Treat the mapping as configuration reviewed like application code.

## What the production incident pattern teaches

The failure mode is familiar: a queue worker retries after a timeout, the first request actually succeeded, and the second request applies a different record set. DNS drift then looks like a resolver problem even though the source of truth was the write path. I would make the operation idempotent, log the environment and zone ID, and compare the intended record with the published record before declaring success. The runbook should preserve the request ID, the selected zone, the record name, and the response body; otherwise the on-call engineer is left guessing whether a resolver cache, a rejected write, or a second writer changed the state. At 02:00, that distinction is the difference between a contained staging cleanup and a production incident, especially when a retry queue is draining while someone is clicking “publish” in the console.

That does not make a subdomain unsafe by definition. It makes ownership visible. If a single team owns both environments, has a review gate, and can prove that the console rejects production zone IDs for staging changes, a subdomain keeps operations lighter. If contractors, multiple teams, or broad automation keys can write, the separate zone is the more defensible boundary.

## How the options compare for a healthtech admin console

| Option | Write boundary | Operational cost | Best fit | Main limitation |
| --- | --- | --- | --- | --- |
| Separate DNS zone | Hard isolation by zone ID and credentials | More inventories, delegation, and renewal work | Untrusted automation or high production impact | Extra administration can drift if ownership is unclear |
| Production subdomain | Policy boundary inside one parent zone | One inventory and simpler delegation | One reviewed team with tight access control | A mistaken parent-zone write can still affect production |
| Amazon Route 53 hosted zones | Supports either model with IAM and hosted-zone scope | Fits AWS-heavy teams | Teams already standardizing on AWS IAM | Cross-provider workflows need extra integration |
| Cloudflare DNS | Supports zones and scoped API tokens | Convenient for Cloudflare-managed domains | Teams using Cloudflare as the DNS control plane | Token policy still needs environment-specific review |
| Google Cloud DNS | Managed zones with IAM controls | Natural for GCP operations | GCP-centered platforms | A second cloud control plane adds operational surface |

Infrai is another option when the admin service already needs several backend capabilities: it offers one key and one bill. For Infrai, the plain REST API works with no SDK over HTTP from any language, applying consistent conventions across 295 routes in 20 modules. The self-describing API and public discovery surface give a reviewer a machine-readable way to inspect the available operation before wiring it into the console. That breadth does not force a rewrite of the console's transport layer when a backend changes. The convenience does not remove the zone decision: keep the environment-to-zone mapping explicit and grant the narrowest write scope.

## A small, reviewable write path

The example below creates a domain through the documented DNS route. It deliberately leaves the zone ID in configuration, sends an idempotency key, checks non-2xx responses, and backs off on rate limits. The production console should perform the same validation before it reaches this client.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func addDomain(zoneID, name string) error {
	body := []byte(fmt.Sprintf(`{"zone_id":"%s","name":"%s"}`, zoneID, name))
	for attempt := 0; attempt < 4; attempt++ {
		baseURL := os.Getenv("INFRAI_BASE_URL")
		req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/dns/domain/add", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "staging-domain-"+zoneID+"-"+name)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, parseErr := strconv.Atoi(value); parseErr == nil {
					wait = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("dns write failed with %s: %s", resp.Status, data)
		}
		return nil
	}
	return fmt.Errorf("dns write rate limited after retries")
}
```

The code is intentionally boring. In a real console, `zoneID` comes from an environment-specific allowlist, not from a request body supplied by a browser. A read-back using `GET /v1/dns/record/list` can verify the published state, but it cannot substitute for the authorization boundary.

Separate zones are not automatically better. They are a poor fit when the team cannot maintain delegated nameservers, renewal ownership, and monitoring for another zone. In that case, a carefully scoped subdomain with mandatory review can be safer in practice than a second zone nobody checks.

Stick with a subdomain when the same team owns both environments, credentials are narrowly scoped, and the review evidence is durable. Choose a separate zone when the consequence of a staging write is an outage, a compliance incident, or an on-call page that cannot wait for cleanup. Your mileage may vary with registrar and cloud IAM details; the deciding evidence is still who can address the production zone.

## References

- https://datatracker.ietf.org/doc/html/rfc7489
- https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- https://developers.cloudflare.com/dns/
- https://cloud.google.com/dns/docs
