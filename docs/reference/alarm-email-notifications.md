# Alarm email notifications (SNS)

Each stack whose `ALARM_EMAIL` is set creates one SNS topic and subscribes
that address to it. Every CloudWatch alarm in the stack (DLQ depth,
consumer/poller/re-sort failures, store staleness) publishes there. Without
`ALARM_EMAIL` the alarms are console-only.

`ALARM_EMAIL` is a local-only setting. Put it in the gitignored `.env.local`,
never in the committed env files. The pre-commit hygiene hook rejects it
there, because this repo is public. It is read at `cdk deploy` time, so
changing it requires a redeploy. The topics are per-stack, so the
subscription steps below must be done once per collection.

## The confirmation step (nothing delivers without it)

SNS email subscriptions deliver **nothing**, alarms included, until someone
clicks the "Confirm subscription" link SNS emails to the address. After
deploying, verify:

```bash
TOPIC=$(aws cloudwatch describe-alarms --region us-west-2 \
  --query "MetricAlarms[?starts_with(AlarmName, \`$STACK_NAME\`)].AlarmActions[0] | [0]" \
  --output text)
aws sns list-subscriptions-by-topic --topic-arn "$TOPIC" --region us-west-2 \
  --query 'Subscriptions[].[Endpoint,SubscriptionArn]' --output table
```

`PendingConfirmation` means the link has not been clicked. A real ARN means
you are live. Confirmation links expire after 3 days. Resend one from the
SNS console (topic, then subscription, then **Request confirmation**) or by
re-running `aws sns subscribe` for the same endpoint. That call is
idempotent: it re-sends rather than duplicating.

## Google Groups eat the confirmation email

Using a group address is recommended, so alarms reach a team. It adds
failure modes, all observed in practice. Work through them in order:

1. **Posting permissions.** The group must allow posts from non-members
   ("Anyone on the web can post"). Otherwise mail from
   `no-reply@sns.amazonaws.com` is refused before anyone sees it.
2. **Moderation and spam handling.** Even with posting allowed, "Moderate
   messages from non-members" holds SNS mail in the group's *Pending
   messages* queue, visible to managers only. A spam handling of "Reject
   immediately" silently discards it: no pending entry, no bounce. SNS
   confirmations (automated sender, single link) fit the spam profile
   exactly. Prefer no moderation for this group, or an approved-senders
   exception.
3. After fixing the group settings, **resend the confirmation**. The
   original was already swallowed. Have any member click the link.
4. Still nothing anywhere? A Workspace admin can run **Email Log Search**
   for `no-reply@sns.amazonaws.com` to see the exact disposition.
   Allowlisting `sns.amazonaws.com` and `amazonses.com` at the domain level
   fixes it durably. Do this even if a resend eventually got through. Real
   alarm emails have the same sender profile as the confirmation, and a
   filter that ate one will eat the other.

## Verify end-to-end

```bash
aws sns publish --topic-arn "$TOPIC" --region us-west-2 \
  --subject "test: $STACK_NAME alarm topic" --message "delivery test"
```

The test should land in the group. Note that alarms notify only on state
*transition* (OK to ALARM). An alarm already in ALARM when the subscription
is confirmed sends nothing until it clears and fires again. The publish test
above is how you prove the pipe without waiting for an incident.
