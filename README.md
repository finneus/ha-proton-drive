# Proton Drive

**Disclaimer: This is a third-party application not officially supported by Proton.**

> **Fork** of [LouisBrunner/ha-proton-drive](https://github.com/LouisBrunner/ha-proton-drive) (MPL-2.0) with:
>
> - Proton Drive CLI **0.9.0** (upstream pins 0.5.0) incl. the new `PROTON_DRIVE_CREDENTIALS_STORE` setting
> - the upload/locking/cleanup fixes from [upstream PR #144](https://github.com/LouisBrunner/ha-proton-drive/pull/144),
>   except automatic trashing of "orphaned" files (they are only skipped, see commit history)
> - regular (non pre-release) versions, so HACS offers updates without enabling beta
>
> Changes are offered back upstream where they make sense. Install via HACS custom repository
> `https://github.com/finneus/ha-proton-drive`.

Proton Drive integration for Home Assistant (compatible with HACS).

## Installation

1. Add this repository (`https://github.com/LouisBrunner/ha-proton-drive`) as a custom repository in the HACS menu.

2. Install by clicking this button:

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=LouisBrunner&repository=ha-proton-drive)

## Development

Start the devcontainer with:

```bash
make devcontainer-start
```

Then connect to the container in another terminal:

```bash
make devcontainer
```

You can then setup the dependencies using

```bash
make setup
```

then start running HA (accessible [locally](http://localhost:8123)) with the integration:

```bash
make dev
```

Finally you can lint the integration using:

```bash
make vet
```
