# Steps that need a person

Most of an agent's journey runs from the terminal: create the project, test it, publish it, install it, run it. A few steps need a person in a web browser. This page lists them, who does each one, where, and how a coding assistant should behave when it reaches one.

## Which steps, and who does them

| Step | When it's needed | Who | Where |
|---|---|---|---|
| Sign in | Once per environment, and again when the session expires | You | `novaos-cli auth login` opens your browser |
| Approve a new version | After every `publish`, unless auto-approve applies (below) | A platform admin | `<marketplace-url>/admin/approvals` |
| Install your copy | Once per person and agent, only if you use the web UI | You | `<marketplace-url>/dashboard`. The [install call](installing-and-running.md#2-install-your-own-copy) needs no person, so an assistant can do it for you |

An admin approves in the Approval Queue. A publisher who isn't a platform admin can't approve their own version: the review service refuses it. A platform admin can approve any version, their own included, and the review records that as a self-review.

### When approval is automatic

Admins choose which versions skip the queue on the **Approval Policy** page (`<marketplace-url>/admin/policies`). Its rules are checked in this order, and the first that matches wins:

1. **Always send to the review queue** when the publisher isn't verified, or when the agent's manifest asks for a tool category the admin ticked there (for example HTTP, Browser, Code execution or Connectors).
2. **Auto-approve → Trusted publishers**: approve at once when the publisher's trust level is **Trusted** (set on the **Publishers** page, `<marketplace-url>/admin/publishers`).
3. **Everything else**: either send to the review queue or auto-approve.

A version that fails its security scan is always rejected, and one whose scan didn't fully run always waits for an admin. The page's **What will happen** panel shows the current result of these rules, and each review records which rule decided it.

When a version is auto-approved, `publish-status --wait` reaches `ACTIVE` on its own, and the only step left for a person is signing in. These settings are each environment's admins' decision.

## How to tell that a person is needed

After `publish`, run:

```bash
novaos-cli publish-status <review-id> --wait
```

`--wait` and `--timeout` need `novaos-cli` 2.2.0 or later; with an older CLI, run `publish-status <review-id>` again until it says `ACTIVE`. With `--wait`, it prints each status as it changes. While it shows **Awaiting admin review** (`PENDING_REVIEW` in `--json`), the version is waiting for an admin and nothing in the terminal can move it on. `--wait` gives up after 15 minutes (`--timeout <seconds>` changes that), exits with code 1 and says to run `publish-status` again.

## Approve a version (platform admin)

1. Open `<marketplace-url>/admin/approvals` (**Approval Queue** in the admin console). The filter shows **Pending Review** by default, and each waiting version is marked **In Review**.
2. Open the version. Its page shows the agent, version, publisher, the **Review ID** that `publish` printed, the scan results and the validation report.
3. Choose **Approve** (notes are optional), or **Reject** with a note saying why; the publisher reads it in `publish-status`.

`publish-status` then moves through **Approved (activating)** to **Active**.

## Install your copy

Use the [install call](installing-and-running.md#2-install-your-own-copy) from the terminal: it needs no browser. In the web UI, go to `<marketplace-url>/dashboard`, open **Available Agents**, select **Install Agent** on the agent's card, check the details and select **Install Agent** again. An agent you already have shows **Installed** and **Open Agent** instead. Your copies, each with its **Instance ID** and a **Copy** button, are under **Installed Agents**. If the UI install fails, use the install call.

## How an assistant should behave at these steps

These rules are for a coding assistant (Claude Code, or any other) that builds and publishes an agent for you.

**Stop and ask; never pretend.** At a step that needs a person, the assistant stops. It names the step, the page, who must act and why, then waits. It never says the agent is live while it is still in review, never makes up an instance ID, and never tries to approve its own agent: approving is a person's decision, even when you are a platform admin.

**Say it plainly.** For example:

> This version is waiting for a platform admin to approve it at `<marketplace-url>/admin/approvals`. I won't approve it myself: approving is a person's decision. Tell me when it's approved, and I'll check and carry on.

**Check after the person acts.** When you say it's done, the assistant checks for itself before it goes on:

- after approval: `novaos-cli publish-status <review-id>` shows `ACTIVE`;
- after a UI install: `GET <gateway-url>/api/v1/instances` lists your copy (see [Find your copy](installing-and-running.md#3-find-your-copy)); you can also read its Instance ID under **Installed Agents** on the dashboard;
- after sign-in: `novaos-cli auth whoami` shows your identity.

If the check doesn't confirm it, the assistant says what it sees and waits again.

**Offer browser help, with consent.** If your assistant can control a browser (for example, Claude Code with the Claude in Chrome extension), it can open the page and do a UI step in your signed-in browser, asking you before each action. It still doesn't approve the agent itself: a platform admin decides that. If your assistant can't control a browser, it can suggest an extension that lets it, or you do the step yourself.
