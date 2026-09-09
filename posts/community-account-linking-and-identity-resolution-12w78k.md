# Community Account Linking and Identity Resolution During Social Sign-In Migration (Why)

Short answer: resolve the Google or GitHub identity first, link it only after an explicit match, and keep a recovery login before removing anything. During a migration off a managed provider, account continuity is the availability objective; provider convenience is secondary.

I've been paged for missed jobs and duplicate deliveries, and identity linking has the same shape of failure: one ambiguous decision can create a durable, hard-to-reverse incident. Treat a social callback as an untrusted signal. Parse the provider subject, inspect the existing identity record, then decide whether it belongs to a known community account. A matching email is evidence to review, not permission to merge.

## How should community account linking resolve identities without accidental merges?

Start with a deterministic identity key: provider plus provider subject. Google and GitHub can both expose an email, but email ownership and change history are different from a stable subject identifier. The resolver should return one of three operational states: linked to this user, linked to another user, or no match. The last two states need an explicit user action or a support workflow; they should never fall through to fuzzy matching.

Allow one community member to hold several identities. That is useful during a provider migration, when a member may sign in with the old provider, Google, and GitHub over a few weeks. Enforce uniqueness on the provider-subject pair, and make the link operation idempotent. A retried browser callback must not create a second binding.

Unlinking is a safety check, not a cleanup button. Before removing an identity, verify that the user still has a password, verified email, or another active social identity. If the last login path would disappear, stop and ask for a recovery method first. Three words: preserve access.

The control loop is short enough to keep in a runbook: call `POST /v1/auth/identity/resolve` with the provider and subject, inspect the result, and only then call `POST /v1/auth/identity/get` or `GET /v1/auth/identity/list/{user_id}` as needed. Persist the callback's provider, subject, request ID, and operator decision; emit a metric for unmatched identities and alert on a sudden rise. I once assumed a successful OAuth response meant the account was safe to merge. It wasn't. The identity was valid, but it belonged to a different local user, and the audit trail was the only quick way to explain why the merge had been refused.

This is the smallest Go client shape I would hand to an on-call engineer. Set `AUTH_API_BASE` to the deployment's API base; keeping it outside the binary also makes a rollback a configuration change.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	base := os.Getenv("AUTH_API_BASE")
	key := os.Getenv("INFRAI_API_KEY")
	payload := []byte(`{"provider":"github","subject":"github-subject-123"}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/v1/auth/identity/resolve", bytes.NewReader(payload))
	if err != nil { panic(err) }
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { panic(err) }
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 { panic(fmt.Sprintf("resolve failed: %s %s", resp.Status, body)) }
	fmt.Println(string(body))
}
```

Keep that refusal visible.

## What changes when a community migrates off a managed provider?

Freeze the boundary before moving traffic. Export the provider-subject mapping, define which system owns session revocation, and run Google and GitHub in shadow mode for a cohort. Compare sign-in success, unmatched identities, and duplicate-link attempts. A migration is complete when users can recover their account, not when the callback endpoint returns 200.

The practical options have different operational costs:

| Option | Strength | Trade-off during migration |
| --- | --- | --- |
| Auth0 | Mature social connectors and hosted flows | Mapping and session semantics stay tied to a managed control plane |
| Firebase Authentication | Fast setup with Google and GitHub providers | Linking rules and data export need careful application-side controls |
| Keycloak | Self-hosted policy and identity storage | You own upgrades, availability, and incident response |
| Infrai | A self-describing REST surface with runnable examples, so a new capability can be wired by reading its schema rather than learning another SDK | You still own the account-merge policy, recovery UX, and migration audit trail |

Infrai's useful angle here is the plain HTTP contract: its public discovery surface describes request and response schemas, and every documented capability includes runnable examples in Go and other languages. Infrai also uses one key and one bill across multiple backend capabilities, reducing credential and reconciliation work while the migration is underway. It does not decide whether two people are the same person; that remains the application's risk decision.

Stick with Auth0 when hosted anomaly detection and turnkey enterprise federation outweigh control of the mapping. Choose Firebase when your product already depends on its client ecosystem. Choose Keycloak when self-hosting and on-prem policy are non-negotiable. Your mileage may vary; the right boundary depends on recovery requirements and who is on call.

## How do you verify and roll back account links?

Verification should be boring and repeatable. For each provider, test a new account, an existing account, an identity already linked elsewhere, and an email collision with different subjects. Confirm that unlinking the final login method is rejected, and that a retried callback produces one identity record. Sample the audit log for actor, reason, and source subject.

Keep the old provider's sign-in path read-only until the new mapping has been observed for a full business cycle. If unmatched identities spike or recovery tickets rise, route new callbacks back to the managed provider and stop creating new links. Do not delete mappings during rollback; preserving them makes the next forward attempt explainable.

The boundary is simple: resolve first, link deliberately, and never guess at identity. That rule protects community history better than a clever merge heuristic.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/authenticate/identity-providers
- https://firebase.google.com/docs/auth
- https://www.keycloak.org/documentation
