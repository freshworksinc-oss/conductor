# Freshworks branding assets (ui-next)

Tab favicon and optional PWA icons for internal Conductor UI.

## What ships here

- `favicon.ico` — used in `index.html` and `context.js` (`FAVICON_URL`).

## Official logo sources

Use assets from your **internal brand / design system**, not random web downloads, for production:

1. **Freshworks brand portal** (marketing / design team — official logo packs).
2. **Company wiki or Figma** shared by CE/CaaS design.
3. **ceapps-infra-templates** or sibling repos if a Conductor-specific FW mark already exists.

Replace `favicon.ico` (and add PNG sizes under this folder if needed) then rebuild the ui-next image.

## Runtime overrides (`public/context.js`)

| Key | Purpose |
|-----|---------|
| `FAVICON_URL` | Path to tab icon, e.g. `/icons/freshworks/favicon.ico` |
| `DOCUMENT_TITLE` | Browser tab title |
| `CUSTOM_LOGO_URL` | Sidebar header logo URL (see `OpenedLogo`) |
| `SHOW_SIDEBAR_VERSION_BLOCK` | `false` hides bottom Orkes version box |
