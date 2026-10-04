# Sky Backcraft desktop app

<!-- impeccable:product-schema 1 -->

## Platform

macOS native desktop and menu bar app.

## Users

The operator runs the local Sky Backcraft MCP server and connects it to ChatGPT.

## Product Purpose

Configure and operate the MCP server without a terminal or project-folder chooser.

## Capabilities and Constraints

- Three settings: optional public domain, local port, and OAuth or no authentication.
- Requested initial domain: `skybackcraft.store`; local port: `8130`.
- Native settings window and menu bar controls follow the supplied macOS screenshots.
- The app owns its backend children, settings, logs, and application data.
- An existing named tunnel routes the public domain to the local port. An empty domain uses a temporary Cloudflare tunnel.
- OAuth must use discovery, authorization codes, PKCE, owner consent, and expiring bearer tokens.
- Success requires app interaction, real MCP requests, and an actual ChatGPT tool invocation. Public DNS and account permissions are separate dependencies.

## Stack

Swift/AppKit with the existing Rust MCP backend. Native system frameworks avoid introducing a GUI dependency or web wrapper.
