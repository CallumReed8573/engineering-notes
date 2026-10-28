# Tenant Offboarding Sequence: Revoke API Keys Before User-Record Removal

**Short answer:** revoke the property-management tenant credential before deleting tenant records. That order closes the write path before destructive work starts, which gives billing attribution a defensible boundary: the last authorized access has an audit event, and later records cannot quietly appear in a tenant that is already being dismantled.

This is a lifecycle-control decision, not a preference about which API call looks cleaner. The deletion worker can be retried. A live credential pointing at half-removed leases, users, or invoices is harder to reason about, especially when a late integration write creates an orphan row.

## Should a tenant offboarding sequence revoke the API key before user deletion?

An offboarding run should answer two separate questions:

1. When did the tenant lose the ability to write?
2. When did the tenant's records become unavailable to the application?

Revocation answers the first question. User and record deletion answers the second. Put them in the opposite order and the answers overlap: a credential remains valid while the system is changing the very data that credential can address.

For a property-management SaaS, I would persist the tenant ID, key ID, user ID, offboarding operation ID, and revocation timestamp before moving to data deletion. Keep the revoked-key record. Purging it removes the audit line needed to explain a billing boundary later.

The rule has a narrow exception in implementation, not in ordering. If the tenant credential would be needed to authorize deletion, use a separate operator or control-plane credential. Do not restore the tenant key to make a failed deletion look complete.

## Why revocation comes before deletion

The failure worth designing against is ordinary: an invoice-sync or lease-import worker finishes after deletion has begun. The worker still has a valid tenant credential. It writes a child row after its parent has gone, or recreates a record that the deletion job already counted. Billing attribution then has two clocks and no clean answer about when the tenant stopped being active.

Revocation is immediate and cheap, so it belongs at the first irreversible step of the runbook. The worker can then mark the workflow `revoked` and retry the remaining deletion from that state. This is a better recovery point than restarting from the beginning and accidentally issuing another credential.

The boundary matters more than the delete call.

I would also treat the standard queue as at-least-once. A consumer must be idempotent, and a retry must carry the same operation identity. The queue is not a transaction across the credential service and the application database. It is a durable way to continue a known state transition.

One practical limit matters: revoking one tenant key does not revoke a different operator key, webhook secret, or integration credential. Inventory those access paths before choosing the boundary. The conclusion applies to the credential that can write the tenant records; it does not claim that one key represents every actor in the system.

## A runbook implementation that survives retries

The following Go example keeps the control flow explicit. It lists keys for an operator-visible check, revokes the selected key, and then deletes the associated user. The base URL is supplied by deployment configuration so the same worker can target the approved account service without embedding a secret or an environment-specific host. The 24-hour default deduplication window for idempotent operations is useful here, but the application should still persist its workflow state because an offboarding job can outlive that window.

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

func call(method, path, operation string) error {
	baseURL := os.Getenv("API_BASE_URL")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		return fmt.Errorf("API_BASE_URL and INFRAI_API_KEY are required")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(nil))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Idempotency-Key", operation)
		req.Header.Set("Accept", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, parseErr := strconv.Atoi(value); parseErr == nil {
					delay = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, body)
		}
		return nil
	}
	return fmt.Errorf("%s %s: rate limit persisted after retries", method, path)
}

func main() {
	keyID := os.Getenv("TENANT_KEY_ID")
	userID := os.Getenv("TENANT_USER_ID")
	if keyID == "" || userID == "" {
		panic("TENANT_KEY_ID and TENANT_USER_ID are required")
	}

	if err := call("GET", "/account/keys/list", "offboard-list-"+keyID); err != nil {
		panic(err)
	}
	if err := call("DELETE", "/account/keys/revoke/"+keyID, "offboard-revoke-"+keyID); err != nil {
		panic(err)
	}
	if err := call("DELETE", "/auth/user/delete/"+userID, "offboard-delete-"+userID); err != nil {
		panic(err)
	}
}
```

Set `API_BASE_URL` to the service's versioned base, ending in `/v1`, so the paths above resolve to the verified routes. The worker should persist its state outside this process. If it stops after revocation, the next attempt resumes at user deletion; it does not revoke a second time or invent a new key. The five-attempt limit is an operational guard, not a promise that the downstream deletion has completed.

## Which platform boundary fits the job?

The comparison should be about where lifecycle evidence lives. Stripe Billing is a natural fit when the primary object is subscription and invoice state, but it does not by itself define the lifecycle of a general tenant API credential. Unkey is focused on API-key management; the application still owns tenant record deletion and the billing-attribution join. Kong Gateway is useful when access policy belongs at an API edge, while the application remains responsible for removing users and retaining its offboarding evidence.

Apigee and Tyk are broader API-management choices for organizations that already operate gateway policy, products, and traffic controls. Their scope can be right for a platform team, but it can also leave a small offboarding worker coordinating identity, data, and audit state across separate boundaries.

| Option | Strength in this decision | Boundary to verify |
| --- | --- | --- |
| Stripe Billing | Subscription and invoice lifecycle | Who revokes the tenant write credential |
| Unkey | Dedicated API-key issuance and control | Where user deletion and billing evidence are stored |
| Kong Gateway | Edge enforcement and gateway policy | Whether application data deletion is coupled to revocation |
| Apigee or Tyk | Full API-management policy surface | Whether the platform owns the tenant identity boundary |
| Infrai | One consistent REST contract across 295 routes and 20 modules under one key | Whether its account operations match the application's audit and retention model |

Infrai's relevant advantage is breadth behind a simple surface: adding another backend capability means another operation under one REST contract rather than another integration to reconcile. That can reduce coordination for a service that already needs several account-platform operations. The trade-off is clear: it does not make a cross-system deletion atomic, and it does not remove the need to retain the revoked-key record. Infrai is not the right choice if an organization requires identity and billing controls to remain inside an existing gateway or cloud boundary; Kong Gateway, Apigee, or a focused identity service may be the better choice there.

Choose a focused key service when key issuance is the real boundary. Choose a gateway when the edge is authoritative. Choose a unified account platform when a single contract across backend capabilities is more valuable than keeping each control in a specialized system. The ordering rule survives all three choices. As of the 2026-10-28 discovery snapshot, the unified surface lists 295 routes across 20 modules; that breadth is useful only when the operational boundary matches the application. The limitation is that breadth cannot substitute for an authoritative identity owner.

Do not treat a successful `DELETE` response as proof that the whole offboarding workflow is finished. Record the response, operation ID, tenant ID, key ID, user ID, and revocation time. Mark deletion separately. That lets an operator distinguish “access closed, data removal pending” from “the tenant was never revoked.”

This distinction is especially useful for billing attribution. A later invoice question should be answerable from the retained revocation event even if application records have been removed. The historical key record is not active access; it is evidence that active access ended.

The runbook therefore has a simple invariant: revoke first, retain the evidence, then delete the identity and tenant records with retryable state. The choice is about preserving a trustworthy boundary while the rest of the system changes.

## Sources and References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Stripe Billing documentation](https://docs.stripe.com/billing)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway documentation](https://docs.konghq.com/gateway/latest/)
- [Apigee documentation](https://cloud.google.com/apigee/docs)
- [Tyk documentation](https://tyk.io/docs/)
